"""monitor.py v2 — 服务器实时监控模块（小时级历史 + 新用户统计）"""
from __future__ import annotations

import os, smtplib, ssl, threading, time
from collections import deque
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Any

try:
    import psutil
    _HAS_PSUTIL = True
except ImportError:
    _HAS_PSUTIL = False

# ── 阈值配置 ─────────────────────────────────────────────────
CPU_THR        = float(os.environ.get('QC_MON_CPU_THR',  '85'))
MEM_THR        = float(os.environ.get('QC_MON_MEM_THR',  '90'))
DISK_THR       = float(os.environ.get('QC_MON_DISK_THR', '90'))
ALERT_INTERVAL = float(os.environ.get('QC_MON_INTERVAL', '300'))

# ── 请求滑动窗口 ──────────────────────────────────────────────
# 每条: (timestamp, user, path, ip)
_req_lock   = threading.Lock()
_req_window: deque = deque()   # 最近24小时
_WINDOW_SEC = 86400            # 24h
_ONLINE_SEC = 300              # 5min内算在线

# ── 小时级历史环形缓冲 ────────────────────────────────────────
# 每条: {'ts':str, 'cpu':float, 'mem':float, 'disk':float, 'reqs':int, 'online':int}
_hist_lock = threading.Lock()
_hist: deque = deque(maxlen=2880)         # 最近48小时（每60s一条 × 60min × 48h）

# ── 告警状态 ──────────────────────────────────────────────────
_alert_lock = threading.Lock()
_last_alert: dict[str, float] = {}

# ── 指标缓存（由监控线程更新，API直接读取，避免阻塞）────────────
_metrics_lock = threading.Lock()
_cached_metrics: dict = {}


def get_cached_metrics() -> dict:
    """返回监控线程最近一次采集的指标缓存，无阻塞。"""
    with _metrics_lock:
        return dict(_cached_metrics) if _cached_metrics else get_metrics()


def _update_metrics_cache(m: dict) -> None:
    with _metrics_lock:
        _cached_metrics.clear()
        _cached_metrics.update(m)


def record_request(*, user: str = '', path: str = '', ip: str = '') -> None:
    now = time.time()
    with _req_lock:
        _req_window.append((now, user or 'anon', path, ip))
        while _req_window and now - _req_window[0][0] > _WINDOW_SEC:
            _req_window.popleft()


def get_online_stats() -> dict[str, Any]:
    now = time.time()
    with _req_lock:
        recent  = [(ts,u,p,ip) for ts,u,p,ip in _req_window if now-ts <= _ONLINE_SEC]
        h1      = [(ts,u,p,ip) for ts,u,p,ip in _req_window if now-ts <= 3600]
        total   = len(_req_window)
        # 按小时统计最近24小时请求量
        hourly: dict[int, int] = {}
        for ts,*_ in _req_window:
            bucket = int((now - ts) // 3600)   # 0=最近1h, 1=1~2h ago ...
            if bucket < 24:
                hourly[bucket] = hourly.get(bucket, 0) + 1

    online_users: set[str] = set()
    for _,u,_,ip in recent:
        if u and u != 'anon': online_users.add(u)
        elif ip: online_users.add(f'ip:{ip}')

    # 用户详情：最近请求时间 + 路径
    user_detail: dict[str, dict] = {}
    for ts,u,p,ip in recent:
        key = u if (u and u != 'anon') else f'ip:{ip}'
        if key not in user_detail or ts > user_detail[key]['last_ts']:
            user_detail[key] = {'name': key, 'last_ts': ts, 'last_path': p,
                                'last_time': datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()}

    with _req_lock:
        qps = sum(1 for ts,*_ in _req_window if now-ts <= 60)

    # 按小时列表（0=最近1h, ..., 23=23~24h ago），转为时序（旧→新）
    hourly_list = [hourly.get(23-i, 0) for i in range(24)]

    return {
        'online_count':  len(online_users),
        'online_users':  sorted(online_users),
        'user_detail':   sorted(user_detail.values(), key=lambda x: -x['last_ts']),
        'requests_1h':   len(h1),
        'requests_5m':   len(recent),
        'requests_24h':  total,
        'qps_1m':        round(qps / 60, 3),
        'hourly_reqs':   hourly_list,   # list[24] 旧→新
    }


def get_metrics() -> dict[str, Any]:
    ts = datetime.now(timezone.utc).isoformat()
    if not _HAS_PSUTIL:
        return {'ts': ts, 'error': 'psutil not installed — run: pip install psutil',
                'cpu_pct': None, 'mem_pct': None, 'disk_pct': None}

    cpu  = psutil.cpu_percent(interval=None)  # 非阻塞，使用上次调用后的累计值
    mem  = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    boot = psutil.boot_time()
    uptime = (time.time() - boot) / 3600
    try:    load = [round(x,2) for x in psutil.getloadavg()]
    except: load = None

    # 网络 IO
    try:
        net = psutil.net_io_counters()
        net_s = {'bytes_sent_gb': round(net.bytes_sent/1e9,3),
                 'bytes_recv_gb': round(net.bytes_recv/1e9,3),
                 'packets_sent':  net.packets_sent,
                 'packets_recv':  net.packets_recv}
    except: net_s = {}

    return {
        'ts':            ts,
        'cpu_pct':       round(cpu, 1),
        'mem_pct':       round(mem.percent, 1),
        'mem_used_gb':   round(mem.used/1e9, 2),
        'mem_total_gb':  round(mem.total/1e9, 2),
        'mem_avail_gb':  round(mem.available/1e9, 2),
        'disk_pct':      round(disk.percent, 1),
        'disk_free_gb':  round(disk.free/1e9, 2),
        'disk_total_gb': round(disk.total/1e9, 2),
        'disk_used_gb':  round(disk.used/1e9, 2),
        'load_avg':      load,
        'uptime_h':      round(uptime, 1),
        'proc_count':    len(psutil.pids()),
        'net':           net_s,
        'psutil_ok':     True,
    }


def get_history() -> list[dict]:
    """返回最近48小时的小时级历史记录（旧→新）。"""
    with _hist_lock:
        return list(_hist)


def get_new_users(db_path: str, hours: int = 24) -> list[dict]:
    """查询最近 hours 小时内注册的用户列表。"""
    try:
        import sqlite3
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        with sqlite3.connect(db_path, timeout=5) as conn:
            rows = conn.execute(
                "SELECT username, email, created_at FROM users WHERE created_at >= ? ORDER BY created_at DESC",
                (cutoff,)
            ).fetchall()
        return [{'username': r[0], 'email': r[1] or '', 'created_at': r[2]} for r in rows]
    except Exception as e:
        return [{'error': str(e)}]


def get_user_count_by_hour(db_path: str, hours: int = 24) -> list[int]:
    """返回最近hours小时每小时新注册数（旧→新），单次查询。"""
    try:
        import sqlite3
        from datetime import timedelta
        now = datetime.now(timezone.utc)
        cutoff = (now - timedelta(hours=hours)).isoformat()
        with sqlite3.connect(db_path, timeout=5) as conn:
            rows = conn.execute(
                "SELECT created_at FROM users WHERE created_at >= ? ORDER BY created_at",
                (cutoff,)
            ).fetchall()
        # 归入每个小时桶
        buckets = [0] * hours
        for (ts,) in rows:
            try:
                dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                diff_h = int((now - dt).total_seconds() // 3600)
                if 0 <= diff_h < hours:
                    buckets[hours - 1 - diff_h] += 1
            except Exception:
                pass
        return buckets
    except Exception:
        return [0] * hours


def _record_history(metrics: dict, online_count: int, req_count: int) -> None:
    """每次调用都追加一条记录（由监控线程每interval秒调用一次）。"""
    now = datetime.now(timezone.utc)
    entry = {
        'ts':     now.strftime('%m-%d %H:%M'),
        'cpu':    metrics.get('cpu_pct', 0),
        'mem':    metrics.get('mem_pct', 0),
        'disk':   metrics.get('disk_pct', 0),
        'online': online_count,
        'reqs':   req_count,
    }
    with _hist_lock:
        _hist.append(entry)


def _should_alert(key: str) -> bool:
    now = time.time()
    with _alert_lock:
        if now - _last_alert.get(key, 0) < ALERT_INTERVAL:
            return False
        _last_alert[key] = now
        return True


def _send_alert_email(subject: str, body: str) -> None:
    smtp_host = os.environ.get('QC_SMTP_HOST', 'smtp.qq.com')
    smtp_port = int(os.environ.get('QC_SMTP_PORT', '465'))
    smtp_user = os.environ.get('QC_SMTP_USER', '')
    smtp_pass = os.environ.get('QC_SMTP_PASS', '')
    to_addr   = os.environ.get('QC_MON_ALERT_TO', smtp_user)
    if not smtp_user or not smtp_pass or not to_addr:
        print(f'[MON] 告警未发送（SMTP未配置）: {subject}', flush=True); return
    try:
        msg = EmailMessage()
        msg['Subject'] = f'[选币器告警] {subject}'
        msg['From'] = smtp_user; msg['To'] = to_addr
        msg.set_content(body)
        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtp_host, smtp_port, context=ctx, timeout=15) as s:
            s.login(smtp_user, smtp_pass); s.send_message(msg)
        print(f'[MON] 告警邮件已发送: {subject}', flush=True)
    except Exception as e:
        print(f'[MON] 告警邮件发送失败: {e}', flush=True)


def check_alerts(metrics: dict | None = None) -> list[str]:
    if metrics is None: metrics = get_metrics()
    triggered = []
    cpu = metrics.get('cpu_pct')
    if cpu is not None and cpu >= CPU_THR and _should_alert('cpu'):
        _send_alert_email(f'CPU使用率 {cpu}%',
            f'CPU使用率告警\n当前: {cpu}%  阈值: {CPU_THR}%\n时间: {metrics["ts"]}')
        triggered.append(f'cpu:{cpu}%')
    mem = metrics.get('mem_pct')
    if mem is not None and mem >= MEM_THR and _should_alert('mem'):
        _send_alert_email(f'内存使用率 {mem}%',
            f'内存告警\n当前: {mem}%  阈值: {MEM_THR}%\n已用: {metrics.get("mem_used_gb")}GB/{metrics.get("mem_total_gb")}GB\n时间: {metrics["ts"]}')
        triggered.append(f'mem:{mem}%')
    disk = metrics.get('disk_pct')
    if disk is not None and disk >= DISK_THR and _should_alert('disk'):
        _send_alert_email(f'磁盘使用率 {disk}%',
            f'磁盘告警\n当前: {disk}%  阈值: {DISK_THR}%\n剩余: {metrics.get("disk_free_gb")}GB\n时间: {metrics["ts"]}')
        triggered.append(f'disk:{disk}%')
    return triggered


# ── 后台监控线程 ──────────────────────────────────────────────
_monitor_thread: threading.Thread | None = None
_monitor_stop = threading.Event()


def start_monitor_thread(interval: float = 60.0) -> None:
    global _monitor_thread
    if _monitor_thread and _monitor_thread.is_alive():
        return

    def _loop():
        print(f'[MON] 监控线程启动 interval={interval}s cpu_thr={CPU_THR}% mem_thr={MEM_THR}% disk_thr={DISK_THR}%', flush=True)
        # 立即采集第一个历史快照，避免走势图长时间空白
        try:
            # 先调用一次 interval=0.1 初始化 cpu_percent 基准值
            if _HAS_PSUTIL:
                psutil.cpu_percent(interval=0.1)
            m = get_metrics()
            _update_metrics_cache(m)
            o = get_online_stats()
            _record_history(m, o['online_count'], o['requests_1h'])
            print(f'[MON] 初始快照: cpu={m.get("cpu_pct")} mem={m.get("mem_pct")} hist_len={len(_hist)}', flush=True)
        except Exception as e:
            print(f'[MON] 初始快照失败: {e}', flush=True)
        while not _monitor_stop.wait(interval):
            try:
                m = get_metrics()
                _update_metrics_cache(m)
                o = get_online_stats()
                _record_history(m, o['online_count'], o['requests_1h'])
                alerts = check_alerts(m)
                if alerts:
                    print(f'[MON] 告警触发: {alerts}', flush=True)
            except Exception as e:
                print(f'[MON] 监控线程异常: {e}', flush=True)

    _monitor_thread = threading.Thread(target=_loop, daemon=True, name='qc-monitor')
    _monitor_thread.start()


def stop_monitor_thread() -> None:
    _monitor_stop.set()