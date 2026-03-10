from __future__ import annotations

import json
import os
from collections import OrderedDict
import smtplib
import ssl
import sys
import threading
import time
import gzip
import logging
from datetime import datetime, timedelta
from email import policy
from email.message import EmailMessage
from email.utils import parseaddr
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse
from http import cookies


repo_root = Path(__file__).resolve().parents[3]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))


from apps.crypto_screener.app.pipeline import default_paths, run_once  # noqa: E402
from apps.crypto_screener.app.filter_engine import apply_all_filters, assign_rank, sort_rows, ema, sma, rsi, rolling_std, supertrend, kdj, obv_with_ma, stoch_rsi  # noqa: E402
from apps.crypto_screener.app.expr_lang import parse_expr_tokens, tokenize_expr, eval_ast  # noqa: E402
from apps.crypto_screener.app.series_source import load_symbol_series  # noqa: E402
from apps.crypto_screener.app.wecom_sender import send_image, send_markdown  # noqa: E402
from apps.crypto_screener.app.wecom_report import render_selection_png  # noqa: E402
from apps.db.auth_sqlite import (  # noqa: E402
    AuthConfig,
    cleanup_email,
    cleanup_sessions,
    cleanup_sms,
    create_email_code,
    create_sms_code,
    create_session,
    default_db_path,
    delete_session,
    get_wecom_config,
    get_user_by_username_or_phone,
    get_user_by_session,
    init_db,
    list_enabled_wecom_configs,
    list_custom_factors,
    register_user,
    replace_custom_factors,
    upsert_custom_factor,
    delete_custom_factor,
    upsert_wecom_config,
    verify_email_code,
    verify_sms_code,
    verify_user_identity_password,
)
from apps.db.aliyun_sms import load_from_env as load_aliyun_sms, send_code as aliyun_send_code  # noqa: E402


run_lock = threading.Lock()
wecom_lock = threading.Lock()
wecom_state = {"last_hour_key": None, "last_meta_updated_at": None}
gz_cache_lock = threading.Lock()
gz_cache: dict[str, tuple[str, bytes]] = {}
latest_cache_lock = threading.Lock()
latest_cache: tuple[str, dict] | None = None
expr_cache_lock = threading.Lock()
expr_cache: dict[str, dict] = {}
series_cache_lock = threading.Lock()
series_cache: OrderedDict[tuple[str, str, int], dict] = OrderedDict()
enriched_cache_lock = threading.Lock()
enriched_cache: OrderedDict[tuple[int, str, str, int, int], dict] = OrderedDict()
update_state = {
    "running": False,
    "last_started": None,
    "last_finished": None,
    "last_error": None,
    "last_error_preprocess": None,
}
pkl_build_lock = threading.Lock()
pkl_build_state = {
    "running": False,
    "last_started": None,
    "last_finished": None,
    "last_error": None,
}

AUTH_COOKIE = "qc_sess"
SERVER_INFO = {"pid": os.getpid(), "boot": datetime.now().isoformat(timespec="seconds")}
logger = logging.getLogger("qc_screener")
if not logger.handlers:
    h = logging.StreamHandler(stream=sys.stdout)
    h.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(h)
logger.setLevel(getattr(logging, (os.environ.get("QC_LOG_LEVEL") or "INFO").upper(), logging.INFO))

metrics_lock = threading.Lock()
metrics = {"count": 0, "errors": 0, "by": {}}

auth_cfg = AuthConfig(
    db_path=Path(os.environ.get("QC_SCREENER_DB") or default_db_path(repo_root)),
    session_ttl_days=int(os.environ.get("QC_SCREENER_SESSION_DAYS") or "14"),
    sms_code_pepper=os.environ.get("QC_SMS_PEPPER") or None,
)


def _sms_missing_env() -> list[str]:
    akid = (
        os.environ.get("QC_ALIYUN_SMS_PREFIX")
        or os.environ.get("QC_ALIYUN_ACCESS_KEY_ID")
        or os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_ID")
        or ""
    ).strip()
    aks = (
        os.environ.get("QC_ALIYUN_SMS_EKEY")
        or os.environ.get("QC_ALIYUN_ACCESS_KEY_SECRET")
        or os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
        or ""
    ).strip()
    sign = (os.environ.get("QC_ALIYUN_SMS_SIGN_NAME") or "").strip()
    t_reg = (os.environ.get("QC_ALIYUN_SMS_TEMPLATE_REGISTER") or "").strip()
    t_log = (os.environ.get("QC_ALIYUN_SMS_TEMPLATE_LOGIN") or "").strip()
    missing: list[str] = []
    if not akid:
        missing.append("QC_ALIYUN_SMS_PREFIX")
    if not aks:
        missing.append("QC_ALIYUN_SMS_EKEY")
    if not sign:
        missing.append("QC_ALIYUN_SMS_SIGN_NAME")
    if not t_reg:
        missing.append("QC_ALIYUN_SMS_TEMPLATE_REGISTER")
    if not t_log:
        missing.append("QC_ALIYUN_SMS_TEMPLATE_LOGIN")
    return missing


def _smtp_missing_env() -> list[str]:
    host = (os.environ.get("QC_SMTP_HOST") or "").strip()
    user = (os.environ.get("QC_SMTP_USER") or "").strip()
    pwd = (os.environ.get("QC_SMTP_PASS") or "").strip()
    missing: list[str] = []
    if not host:
        missing.append("QC_SMTP_HOST")
    if not user:
        missing.append("QC_SMTP_USER")
    if not pwd:
        missing.append("QC_SMTP_PASS")
    return missing


def _format_email_addr(raw: str) -> str:
    _name, addr0 = parseaddr(str(raw or ""))
    addr = _normalize_email_addr(addr0)
    if not addr:
        return ""
    return addr


def _normalize_email_addr(raw: str) -> str:
    _name, addr0 = parseaddr(str(raw or ""))
    addr = str(addr0 or "").strip()
    if not addr or "@" not in addr:
        return ""
    local, domain = addr.rsplit("@", 1)
    local = str(local or "").strip()
    domain = str(domain or "").strip().strip(".")
    if not local or not domain:
        return ""
    try:
        local.encode("ascii")
    except Exception:
        return ""
    try:
        domain_ascii = domain.encode("idna").decode("ascii")
    except Exception:
        return ""
    out = f"{local}@{domain_ascii}".lower()
    return out


def _username_ok_public(u: str) -> bool:
    s = (u or "").strip()
    if not s:
        return False
    if len(s) < 3 or len(s) > 32:
        return False
    for ch in s:
        if ch.isalnum():
            continue
        if ch in ("_", "-", "."):
            continue
        return False
    return True


def _send_email_code(*, to_email: str, scene: str, code: str) -> tuple[bool, str]:
    host = (os.environ.get("QC_SMTP_HOST") or "").strip()
    port_raw = (os.environ.get("QC_SMTP_PORT") or "").strip()
    user = (os.environ.get("QC_SMTP_USER") or "").strip()
    pwd = (os.environ.get("QC_SMTP_PASS") or "").strip()
    from_addr = (os.environ.get("QC_SMTP_FROM") or user or "").strip()
    use_ssl = (os.environ.get("QC_SMTP_SSL") or "").strip() == "1"
    use_tls = (os.environ.get("QC_SMTP_TLS") or "1").strip() != "0"
    try:
        port = int(port_raw) if port_raw else (465 if use_ssl else 587)
    except Exception:
        port = 465 if use_ssl else 587
    if not (host and user and pwd and from_addr):
        return False, "email_not_configured"
    to_addr = _normalize_email_addr(to_email)
    user_addr = _normalize_email_addr(user)
    if not to_addr:
        return False, "邮箱格式不合法"
    if not user_addr:
        user_addr = user
    subject = "自动化选币器验证码"
    if scene == "login":
        subject = "自动化选币器登录验证码"
    elif scene == "register":
        subject = "自动化选币器注册验证码"
    body = f"验证码：{code}\n有效期：5分钟\n\n如果不是本人操作，请忽略此邮件。"
    msg = EmailMessage(policy=policy.SMTPUTF8)
    msg["From"] = _format_email_addr(from_addr)
    msg["To"] = _format_email_addr(to_addr)
    msg["Subject"] = str(subject or "")
    msg.set_content(body, charset="utf-8")
    try:
        if use_ssl:
            ctx = ssl.create_default_context()
            with smtplib.SMTP_SSL(host, port, timeout=10, context=ctx) as s:
                s.login(user, pwd)
                s.send_message(msg, from_addr=user_addr, to_addrs=[to_addr])
        else:
            with smtplib.SMTP(host, port, timeout=10) as s:
                s.ehlo()
                if use_tls:
                    ctx = ssl.create_default_context()
                    s.starttls(context=ctx)
                    s.ehlo()
                s.login(user, pwd)
                s.send_message(msg, from_addr=user_addr, to_addrs=[to_addr])
        return True, "ok"
    except smtplib.SMTPAuthenticationError:
        return (
            False,
            "邮箱服务认证失败：请检查 QC_SMTP_USER/QC_SMTP_PASS 是否正确；多数邮箱需要开启SMTP并使用授权码（非网页登录密码），如开启了二次验证需使用应用专用密码。",
        )
    except Exception as e:
        return False, str(e)


def _send_email(*, to_email: str, subject: str, body: str) -> tuple[bool, str]:
    host = (os.environ.get("QC_SMTP_HOST") or "").strip()
    port_raw = (os.environ.get("QC_SMTP_PORT") or "").strip()
    user = (os.environ.get("QC_SMTP_USER") or "").strip()
    pwd = (os.environ.get("QC_SMTP_PASS") or "").strip()
    from_addr = (os.environ.get("QC_SMTP_FROM") or user or "").strip()
    use_ssl = (os.environ.get("QC_SMTP_SSL") or "").strip() == "1"
    use_tls = (os.environ.get("QC_SMTP_TLS") or "1").strip() != "0"
    try:
        port = int(port_raw) if port_raw else (465 if use_ssl else 587)
    except Exception:
        port = 465 if use_ssl else 587
    if not (host and user and pwd and from_addr):
        return False, "email_not_configured"
    to_addr = _normalize_email_addr(str(to_email or "").strip())
    user_addr = _normalize_email_addr(user)
    if not to_addr:
        return False, "邮箱格式不合法"
    if not user_addr:
        user_addr = user
    msg = EmailMessage(policy=policy.SMTPUTF8)
    msg["From"] = _format_email_addr(from_addr)
    msg["To"] = _format_email_addr(to_addr)
    msg["Subject"] = str(subject or "")
    msg.set_content(str(body or ""), charset="utf-8")
    try:
        if use_ssl:
            ctx = ssl.create_default_context()
            with smtplib.SMTP_SSL(host, port, timeout=10, context=ctx) as s:
                s.login(user, pwd)
                s.send_message(msg, from_addr=user_addr, to_addrs=[to_addr])
        else:
            with smtplib.SMTP(host, port, timeout=10) as s:
                s.ehlo()
                if use_tls:
                    ctx = ssl.create_default_context()
                    s.starttls(context=ctx)
                    s.ehlo()
                s.login(user, pwd)
                s.send_message(msg, from_addr=user_addr, to_addrs=[to_addr])
        return True, "ok"
    except smtplib.SMTPAuthenticationError:
        return (
            False,
            "邮箱服务认证失败：请检查 QC_SMTP_USER/QC_SMTP_PASS 是否正确；多数邮箱需要开启SMTP并使用授权码（非网页登录密码），如开启了二次验证需使用应用专用密码。",
        )
    except Exception as e:
        return False, str(e)


def _strip_quote(symbol: str) -> str:
    s = str(symbol or "")
    if s.endswith("-USDT"):
        return s[:-5]
    if s.endswith("USDT"):
        return s[:-4]
    return s


def _load_latest_rows() -> tuple[dict | None, list[dict]]:
    global latest_cache
    latest_path = repo_root / "apps" / "crypto_screener" / "web" / "data" / "latest.json"
    try:
        st = latest_path.stat()
        etag = f"\"{int(st.st_mtime_ns):x}-{int(st.st_size):x}\""
    except Exception:
        return None, []
    with latest_cache_lock:
        cached = latest_cache
    if cached and cached[0] == etag:
        j = cached[1]
        rows = j.get("results") if isinstance(j, dict) else None
        return (j if isinstance(j, dict) else None), (rows if isinstance(rows, list) else [])
    try:
        raw = latest_path.read_text(encoding="utf-8")
        j = json.loads(raw)
    except Exception:
        return None, []
    if not isinstance(j, dict):
        return None, []
    with latest_cache_lock:
        latest_cache = (etag, j)
    rows = j.get("results")
    return j, (rows if isinstance(rows, list) else [])


def _expand_template(template: str, params: list[Any] | None) -> str:
    s = str(template or "")
    ps = params if isinstance(params, list) else []
    for i in range(1, 13):
        v = ps[i - 1] if i - 1 < len(ps) else None
        try:
            x = float(v)
        except Exception:
            continue
        if x != x:
            continue
        s = s.replace(f"n{i}", str(int(x) if float(int(x)) == x else x))
    return s


def _compile_expr(expr: str) -> dict:
    s = str(expr or "")
    with expr_cache_lock:
        hit = expr_cache.get(s)
    if hit is not None:
        return hit
    ast = parse_expr_tokens(tokenize_expr(s))
    with expr_cache_lock:
        expr_cache[s] = ast
    return ast


def _scalar_from_val(v) -> float | None:
    t = getattr(v, "t", None)
    if t == "scalar":
        try:
            import math
        except Exception:
            math = None
        try:
            x = float(getattr(v, "v", None))
        except Exception:
            return None
        if x != x:
            return None
        if math is not None and not math.isfinite(x):
            return None
        return x
    if t == "series":
        xs = getattr(v, "v", None)
        try:
            import numpy as np
        except Exception:
            np = None
        if np is not None and xs is not None and hasattr(xs, "shape"):
            for i in range(int(xs.shape[0]) - 1, -1, -1):
                x = float(xs[i])
                if x == x:
                    return x
            return None
        try:
            arr = list(xs or [])
        except Exception:
            return None
        for i in range(len(arr) - 1, -1, -1):
            try:
                x = float(arr[i])
                if x == x:
                    return x
            except Exception:
                continue
        return None
    return None


def _get_series_cached(*, market: str, symbol: str, tail: int) -> dict | None:
    key = (str(market).lower(), str(symbol), int(tail))
    now0 = time.time()
    try:
        ttl_sec = int(os.environ.get("QC_SERIES_CACHE_TTL_SEC") or "180")
    except Exception:
        ttl_sec = 180
    ttl_sec = max(15, min(3600, ttl_sec))
    try:
        verify_every_sec = int(os.environ.get("QC_SERIES_CACHE_VERIFY_INTERVAL_SEC") or "20")
    except Exception:
        verify_every_sec = 20
    verify_every_sec = max(0, min(600, verify_every_sec))
    verify_enabled = str(os.environ.get("QC_SERIES_CACHE_VERIFY") or "1").strip() != "0"
    with series_cache_lock:
        hit = series_cache.get(key)
        if isinstance(hit, dict):
            ts0 = float(hit.get("ts") or 0.0)
            if now0 - ts0 > ttl_sec:
                series_cache.pop(key, None)
                hit = None
            else:
                series_cache.move_to_end(key)
    if hit is not None:
        payload = hit.get("payload") if isinstance(hit, dict) and "payload" in hit else hit
        try:
            need_verify = False
            if verify_enabled:
                checked_at = float(hit.get("checked_at") or 0.0) if isinstance(hit, dict) else 0.0
                need_verify = (now0 - checked_at) >= float(verify_every_sec)
            if need_verify:
                ver0 = hit.get("ver") if isinstance(hit, dict) else None
                from apps.crypto_screener.app import series_source as ss  # noqa: E402

                cur = ss.get_series_versions(market=key[0], symbol=key[1], tail=key[2], repo_root=repo_root)
                if isinstance(ver0, dict):
                    if int(ver0.get("csv_mtime_ns") or -1) != int(cur.get("csv_mtime_ns") or -1):
                        payload = None
                    elif int(ver0.get("pkl_mtime_ns") or -1) != int(cur.get("pkl_mtime_ns") or -1):
                        payload = None
                if payload is not None and isinstance(hit, dict):
                    hit["checked_at"] = now0
                    with series_cache_lock:
                        if key in series_cache and isinstance(series_cache.get(key), dict):
                            series_cache[key]["checked_at"] = now0
        except Exception:
            pass
        if payload is None:
            hit = None
        else:
            try:
                dt0 = payload.get("dt") if isinstance(payload, dict) else None
                n0 = len(dt0) if isinstance(dt0, list) else 0
                need = int(tail)
                thr = max(30, min(120, need // 3))
                if n0 >= thr:
                    return payload
            except Exception:
                return payload
    s0 = load_symbol_series(market=key[0], symbol=key[1], tail=key[2], repo_root=repo_root)
    if s0 is None:
        return None
    series = s0.series or {}
    latest = {
        "open": None,
        "high": None,
        "low": None,
        "close": None,
        "volume": None,
        "quote_volume": None,
    }
    out_series: dict[str, Any] = {}
    for k in ("open", "high", "low", "close", "volume", "quote_volume"):
        arr0 = series.get(k) or []
        if isinstance(arr0, list):
            arr = arr0
        else:
            try:
                arr = list(arr0)
            except Exception:
                arr = []
        out_series[k] = arr
        for i in range(len(arr) - 1, -1, -1):
            v = arr[i]
            try:
                x = float(v)
                if x == x:
                    latest[k] = x
                    break
            except Exception:
                continue
    src0 = str(getattr(s0, "source", "csv") or "csv")
    payload = {"market": key[0], "symbol": key[1], "dt": s0.dt, "series": out_series, "latest": latest, "source": src0}
    try:
        from apps.crypto_screener.app import series_source as ss  # noqa: E402

        ver = ss.get_series_versions(market=key[0], symbol=key[1], tail=key[2], repo_root=repo_root)
    except Exception:
        ver = None
    with metrics_lock:
        by_src = metrics.get("series_source")
        if not isinstance(by_src, dict):
            by_src = {}
            metrics["series_source"] = by_src
        by_src[src0] = int(by_src.get(src0) or 0) + 1
    try:
        max_items = int(os.environ.get("QC_SERIES_CACHE_MAX") or "2000")
    except Exception:
        max_items = 2000
    max_items = max(200, min(20000, max_items))
    with series_cache_lock:
        series_cache[key] = {"ts": now0, "payload": payload, "ver": ver if isinstance(ver, dict) else {}, "checked_at": now0}
        series_cache.move_to_end(key)
        while len(series_cache) > max_items:
            series_cache.popitem(last=False)
    return payload


def _series_to_list(arr) -> list[float | None]:
    try:
        import math
    except Exception:
        math = None
    out: list[float | None] = []
    try:
        it = arr if arr is not None else []
    except Exception:
        it = []
    for v in it:
        if v is None:
            out.append(None)
            continue
        try:
            x = float(v)
        except Exception:
            out.append(None)
            continue
        if x != x:
            out.append(None)
            continue
        if math is not None and not math.isfinite(x):
            out.append(None)
            continue
        out.append(x)
    return out


def _config_needs_series(config: dict) -> bool:
    try:
        toggles = (config or {}).get("toggles") or {}
        custom_factors = (config or {}).get("customFactors") or []
        sort0 = (config or {}).get("sort") or {}
    except Exception:
        toggles = {}
        custom_factors = []
        sort0 = {}
    for k in ("condEma", "condBollUp", "condBollDown", "condSuper", "condKdj", "condObv", "condStochRsi"):
        try:
            if bool(toggles.get(k)):
                return True
        except Exception:
            continue
    try:
        for f in custom_factors:
            if not isinstance(f, dict):
                continue
            if bool(f.get("enabled")) or bool(f.get("thresholdEnabled")) or bool(f.get("showColumn")):
                return True
    except Exception:
        pass
    sk = str((sort0 or {}).get("key") or "")
    if sk.startswith("_expr."):
        return True
    if sk in ("ema", "boll_up", "boll_down", "supertrend", "kdj_k", "kdj_d", "obv", "obv_ma", "stoch_rsi_k", "stoch_rsi_d"):
        return True
    return False


def _attach_series_to_rows(*, rows: list[dict], config: dict, tail: int, force: bool = False) -> list[dict]:
    if not rows:
        return []
    if (not force) and (not _config_needs_series(config)):
        return rows
    try:
        t = int(tail)
    except Exception:
        t = 720
    t = max(60, min(3650, t))
    out: list[dict] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        m = str(r.get("market") or "").lower()
        s = str(r.get("symbol") or "")
        if not m or not s:
            out.append(r)
            continue
        try:
            ctx = _get_series_cached(market=m, symbol=s, tail=t)
        except Exception:
            ctx = None
        if not isinstance(ctx, dict):
            out.append(r)
            continue
        series0 = ctx.get("series")
        dt0 = ctx.get("dt")
        latest0 = ctx.get("latest") if isinstance(ctx.get("latest"), dict) else {}
        r2 = dict(r)
        if isinstance(series0, dict):
            s_out: dict[str, Any] = {}
            for k in ("open", "high", "low", "close", "volume", "quote_volume"):
                arr = series0.get(k)
                if isinstance(arr, list):
                    s_out[k] = arr
                else:
                    s_out[k] = _series_to_list(arr)
            r2["series"] = s_out
        try:
            c0 = latest0.get("close") if isinstance(latest0, dict) else None
            if c0 is not None:
                r2["close"] = float(c0)
        except Exception:
            pass
        try:
            if isinstance(dt0, list) and dt0:
                r2["dt_close"] = str(dt0[-1])
        except Exception:
            pass
        out.append(r2)
    return out


def _build_wecom_markdown(*, latest: dict | None, rows: list[dict], top_n: int) -> str:
    summary = (latest or {}).get("summary") or {}
    dt = str(summary.get("latest_dt_display") or summary.get("latest_dt_close") or summary.get("generated_at") or "")
    title = f"时间：{dt} ｜ 命中：{len(rows)}\n"
    if not rows:
        return title + "\n无命中\n"
    lines = []
    for r in rows[: max(1, int(top_n))]:
        sym = _strip_quote(str(r.get("symbol") or ""))
        m0 = str(r.get("market") or "").lower()
        m = "现货" if m0 == "spot" else ("合约" if m0 == "swap" else str(r.get("market") or ""))
        close = r.get("close")
        rk = r.get("_rank")
        close_s = f"{float(close):.6g}" if isinstance(close, (int, float)) else (str(close) if close is not None else "")
        lines.append(f"- {rk}. {sym}（{m}） close={close_s}")
    return title + "\n".join(lines)


def _pick_series_tail_for_filters(*, latest: dict | None, config: dict, fallback: int = 720) -> int:
    try:
        base = int((((latest or {}).get("config") or {}).get("tail_len") if isinstance((latest or {}).get("config"), dict) else None) or fallback)
    except Exception:
        base = fallback
    params = (config or {}).get("params") or {}
    toggles = (config or {}).get("toggles") or {}

    def _int0(x) -> int:
        try:
            return int(float(x))
        except Exception:
            return 0

    need = 0
    if toggles.get("condCloseMa"):
        need = max(need, _int0(params.get("maPeriodClose")) + 2)
    if toggles.get("condMa"):
        need = max(need, _int0(params.get("maFast")) + 2, _int0(params.get("maSlow")) + 2)
    if toggles.get("condRsi"):
        need = max(need, _int0(params.get("rsiPeriod")) + 3)
    if toggles.get("condEma"):
        need = max(need, _int0(params.get("emaPeriod")) + 3)
    if toggles.get("condBollUp"):
        need = max(need, _int0(params.get("bollPeriod")) + 3)
    if toggles.get("condBollDown"):
        need = max(need, _int0(params.get("bollDownPeriod")) + 3)
    if toggles.get("condSuper"):
        need = max(need, _int0(params.get("superAtrPeriod")) + 5)
    if toggles.get("condKdj"):
        need = max(need, _int0(params.get("kdjN")) + 3)
    if toggles.get("condObv"):
        need = max(need, _int0(params.get("obvMaPeriod")) + 5)
    if toggles.get("condStochRsi"):
        need = max(need, _int0(params.get("stochRsiP")) + _int0(params.get("stochRsiK")) + 10)

    tail = max(base, 360, need * 4)
    return max(80, min(2160, int(tail)))


def _send_wecom_for_all_enabled() -> None:
    try:
        with wecom_lock:
            hour_key = datetime.now().strftime("%Y%m%d%H")
            if wecom_state.get("last_hour_key") == hour_key:
                return
            wecom_state["last_hour_key"] = hour_key
        latest, all_rows = _load_latest_rows()
        if not all_rows:
            return
        cfgs = list_enabled_wecom_configs(auth_cfg)
        for c in cfgs:
            try:
                webhook_url = str(c.get("webhook_url") or "").strip()
                top_n = max(1, int(c.get("top_n") or 20))
                config = c.get("config") if isinstance(c.get("config"), dict) else {}
                tail0 = _pick_series_tail_for_filters(latest=latest, config=config, fallback=720)
                rows0 = _attach_series_to_rows(rows=all_rows, config=config, tail=tail0)
                r = apply_all_filters(rows0, config)
                selected = r.get("selected") if isinstance(r, dict) else []
                selected = selected if isinstance(selected, list) else []
                sorted_rows, sort_key = sort_rows(selected, config)
                assign_rank(sorted_rows, sort_key, config)
                if not sorted_rows:
                    try:
                        content = _build_wecom_markdown(latest=latest, rows=[], top_n=top_n)
                        send_markdown(webhook_url=webhook_url, content=content)
                    except Exception:
                        pass
                    continue
                try:
                    shown0 = min(top_n, len(sorted_rows))
                    rows_img0 = sorted_rows[:shown0]
                    rows_img = _attach_series_to_rows(rows=rows_img0, config=config, tail=120, force=True)
                    img = render_selection_png(title="", rows=rows_img, top_n=shown0)
                    send_image(webhook_url=webhook_url, image_bytes=img)
                except Exception:
                    pass
            except Exception:
                continue
    except Exception:
        return


def _wecom_scheduler_loop() -> None:
    meta_path = repo_root / "apps" / "crypto_screener" / "web" / "data" / "meta.json"
    while True:
        now = datetime.now()
        next_hour = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
        seconds = max(1.0, (next_hour - now).total_seconds())
        time.sleep(min(60.0, seconds))
        if datetime.now() < next_hour:
            continue
        start = time.time()
        last = None
        with wecom_lock:
            last = wecom_state.get("last_meta_updated_at")
        while True:
            cur = None
            try:
                if meta_path.exists():
                    payload = json.loads(meta_path.read_text(encoding="utf-8"))
                    cur = str(payload.get("updated_at") or "")
            except Exception:
                cur = None
            if cur and cur != str(last or ""):
                with wecom_lock:
                    wecom_state["last_meta_updated_at"] = cur
                _send_wecom_for_all_enabled()
                break
            if time.time() - start >= float(os.environ.get("QC_WECOM_WAIT_META_SECONDS") or "1800"):
                break
            time.sleep(5.0)


def _run_update(fetch: bool) -> None:
    paths = default_paths()
    with run_lock:
        update_state["running"] = True
        update_state["last_started"] = datetime.now().isoformat(timespec="seconds")
        update_state["last_error"] = None
        update_state["last_error_preprocess"] = None
        try:
            run_once(paths, fetch=fetch)
            try:
                threading.Thread(target=_send_wecom_for_all_enabled, daemon=True).start()
            except Exception:
                pass
            try:
                from 数据获取.incremental_update import run_incremental_catchup

                cfg_path = Path(os.environ.get("QC_PREPROCESS_CONFIG") or (repo_root / "数据获取" / "config.yaml"))
                lag_h = int(os.environ.get("QC_PREPROCESS_LAG_HOURS") or "1")
                max_h = int(os.environ.get("QC_PREPROCESS_MAX_HOURS_PER_RUN") or "24")
                run_incremental_catchup(cfg_path, lag_hours=lag_h, max_hours=max_h)
                if str(os.environ.get("QC_BUILD_PKL_CACHE") or "0").strip() != "0":
                    with pkl_build_lock:
                        running = bool(pkl_build_state.get("running"))
                        if not running:
                            pkl_build_state["running"] = True
                            pkl_build_state["last_started"] = datetime.now().isoformat(timespec="seconds")
                            pkl_build_state["last_error"] = None

                            def pkl_worker() -> None:
                                try:
                                    from 数据获取.factor_cache_update import build_market_cache

                                    tail = int(os.environ.get("QC_PKL_CACHE_TAIL") or "2160")
                                    workers = int(os.environ.get("QC_PKL_CACHE_WORKERS") or "8")
                                    build_market_cache(market="swap", tail=tail, symbols_limit=0, workers=workers, incremental=True)
                                    build_market_cache(market="spot", tail=tail, symbols_limit=0, workers=workers, incremental=True)
                                except Exception as e:
                                    with pkl_build_lock:
                                        pkl_build_state["last_error"] = str(e)
                                finally:
                                    with pkl_build_lock:
                                        pkl_build_state["running"] = False
                                        pkl_build_state["last_finished"] = datetime.now().isoformat(timespec="seconds")

                            threading.Thread(target=pkl_worker, daemon=True).start()
            except Exception as e:
                update_state["last_error_preprocess"] = str(e)
        except Exception as e:
            update_state["last_error"] = str(e)
        finally:
            update_state["running"] = False
            update_state["last_finished"] = datetime.now().isoformat(timespec="seconds")
            if update_state.get("last_error") is None:
                pass


def start_update(fetch: bool) -> bool:
    if update_state.get("running"):
        return False

    def worker() -> None:
        _run_update(fetch=fetch)

    threading.Thread(target=worker, daemon=True).start()
    return True


def _scheduler_loop() -> None:
    while True:
        now = datetime.now()
        next_hour = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
        seconds = max(1.0, (next_hour - now).total_seconds())
        time.sleep(min(60.0, seconds))
        if datetime.now() < next_hour:
            continue
        start_update(fetch=True)


class Handler(BaseHTTPRequestHandler):
    def _send_bytes(self, code: int, data: bytes, *, content_type: str, etag: str | None = None, content_encoding: str | None = None) -> None:
        self._resp_code = int(code)
        self._resp_bytes = int(len(data or b""))
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        if etag:
            self.send_header("ETag", etag)
        if content_encoding:
            self.send_header("Content-Encoding", content_encoding)
            self.send_header("Vary", "Accept-Encoding")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _etag_from_file(self, p: Path) -> str:
        st = p.stat()
        return f"\"{int(st.st_mtime_ns):x}-{int(st.st_size):x}\""

    def _maybe_304(self, etag: str) -> bool:
        inm = str(self.headers.get("If-None-Match") or "").strip()
        if inm and inm == etag:
            self.send_response(304)
            self.send_header("ETag", etag)
            self.end_headers()
            return True
        return False

    def _read_json(self) -> dict | None:
        try:
            n = int(self.headers.get("Content-Length") or "0")
        except Exception:
            n = 0
        if n <= 0 or n > 1024 * 1024:
            return None
        raw = self.rfile.read(n)
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return None

    def _cookie(self) -> cookies.SimpleCookie:
        c = cookies.SimpleCookie()
        raw = self.headers.get("Cookie")
        if raw:
            try:
                c.load(raw)
            except Exception:
                pass
        return c

    def _get_session_id(self) -> str | None:
        c = self._cookie()
        if AUTH_COOKIE in c:
            try:
                return str(c[AUTH_COOKIE].value or "")
            except Exception:
                return None
        return None

    def _current_user(self) -> dict | None:
        sid = self._get_session_id()
        if not sid:
            return None
        return get_user_by_session(auth_cfg, session_id=sid)

    def _set_cookie(self, name: str, value: str, *, max_age: int | None) -> None:
        c = cookies.SimpleCookie()
        c[name] = value
        c[name]["path"] = "/"
        c[name]["httponly"] = True
        c[name]["samesite"] = "Lax"
        if max_age is not None:
            c[name]["max-age"] = str(int(max_age))
        if os.environ.get("QC_SCREENER_SECURE_COOKIE") == "1":
            c[name]["secure"] = True
        self.send_header("Set-Cookie", c.output(header="").strip())

    def _send_json(self, code: int, data: dict) -> None:
        raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
        accept = str(self.headers.get("Accept-Encoding") or "")
        use_gzip = "gzip" in accept.lower()
        body = gzip.compress(raw, compresslevel=6) if use_gzip else raw
        self._resp_code = int(code)
        self._resp_bytes = int(len(body or b""))
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        if use_gzip:
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Vary", "Accept-Encoding")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_request(self, code: int | str = "-", size: int | str = "-") -> None:
        try:
            code_i = int(code)
        except Exception:
            code_i = -1
        try:
            size_i = int(size)
        except Exception:
            size_i = int(getattr(self, "_resp_bytes", 0) or 0)
        start = float(getattr(self, "_req_start", time.perf_counter()))
        dur_ms = max(0.0, (time.perf_counter() - start) * 1000.0)
        path0 = (urlparse(self.path).path or "/") if getattr(self, "path", None) else "/"
        rec = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "remote": str(getattr(self, "client_address", ["-"])[0]),
            "method": str(getattr(self, "command", "")),
            "path": path0,
            "code": code_i,
            "ms": round(dur_ms, 3),
            "bytes": size_i,
        }
        try:
            logger.info(json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
        except Exception:
            pass
        k = f"{rec['method']} {path0} {code_i}"
        with metrics_lock:
            metrics["count"] = int(metrics.get("count") or 0) + 1
            if code_i >= 500:
                metrics["errors"] = int(metrics.get("errors") or 0) + 1
            by = metrics.get("by")
            if not isinstance(by, dict):
                by = {}
                metrics["by"] = by
            st = by.get(k)
            if not isinstance(st, dict):
                st = {"count": 0, "ms_sum": 0.0, "ms_max": 0.0, "bytes_sum": 0}
                by[k] = st
            st["count"] = int(st.get("count") or 0) + 1
            st["ms_sum"] = float(st.get("ms_sum") or 0.0) + float(rec["ms"])
            st["ms_max"] = max(float(st.get("ms_max") or 0.0), float(rec["ms"]))
            st["bytes_sum"] = int(st.get("bytes_sum") or 0) + int(rec["bytes"])

    def _sms_missing_env(self) -> list[str]:
        return _sms_missing_env()

    def _redirect(self, location: str) -> None:
        self.send_response(302)
        self.send_header("Location", location)
        self.end_headers()

    def _require_auth(self) -> dict | None:
        user = self._current_user()
        if user:
            return user
        self._send_json(401, {"ok": False, "error": "unauthorized"})
        return None

    def do_POST(self) -> None:
        self._req_start = time.perf_counter()
        parsed = urlparse(self.path)
        path = (parsed.path or "").rstrip("/") or "/"
        path = (parsed.path or "").rstrip("/") or "/"
        if path == "/api/latest_enriched":
            user = self._require_auth()
            if not user:
                return
            payload = self._read_json() or {}
            latest, all_rows = _load_latest_rows()
            if not latest or not all_rows:
                self._send_json(500, {"ok": False, "error": "no_snapshot"})
                return
            req_factors = payload.get("custom_factors")
            factors = req_factors if isinstance(req_factors, list) else list_custom_factors(auth_cfg, user_id=int(user["id"]))
            cfg0 = latest.get("config") if isinstance(latest, dict) else {}
            try:
                tail = int(payload.get("tail") or (cfg0.get("tail_len") if isinstance(cfg0, dict) else None) or 360)
            except Exception:
                tail = 360
            tail = max(80, min(2160, tail))

            try:
                import hashlib
                params = payload.get("params") if isinstance(payload.get("params"), dict) else {}
                toggles = payload.get("toggles") if isinstance(payload.get("toggles"), dict) else {}
                fields_raw = payload.get("fields")
                if isinstance(fields_raw, list):
                    req_fields = [str(x).strip() for x in fields_raw if str(x).strip()]
                elif isinstance(fields_raw, str):
                    req_fields = [x.strip() for x in fields_raw.split(",") if x.strip()]
                else:
                    req_fields = []
                include_debug_raw = payload.get("include_debug")
                include_debug = bool(include_debug_raw) if isinstance(include_debug_raw, bool) else str(include_debug_raw or "0").strip().lower() in ("1", "true", "yes", "on")
                symbols_raw = payload.get("symbols")
                symbols_norm: list[dict[str, str]] = []
                if isinstance(symbols_raw, list):
                    for it in symbols_raw:
                        if isinstance(it, dict):
                            m = str(it.get("market") or "").strip().lower()
                            s = str(it.get("symbol") or "").strip()
                            if m in ("swap", "spot") and s:
                                symbols_norm.append({"market": m, "symbol": s})
                        elif isinstance(it, str):
                            t0 = it.strip()
                            if "|" in t0:
                                m, s = t0.split("|", 1)
                                m = m.strip().lower()
                                s = s.strip()
                                if m in ("swap", "spot") and s:
                                    symbols_norm.append({"market": m, "symbol": s})
                symbols_sig = hashlib.sha1(json.dumps(symbols_norm, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest() if symbols_norm else ""
                try:
                    chunk_offset = max(0, int(payload.get("chunk_offset") or 0))
                except Exception:
                    chunk_offset = 0
                try:
                    chunk_limit = int(payload.get("chunk_limit") or 0)
                except Exception:
                    chunk_limit = 0
                if chunk_limit <= 0:
                    try:
                        chunk_limit = int(os.environ.get("QC_ENRICHED_CHUNK_SIZE") or "120")
                    except Exception:
                        chunk_limit = 120
                chunk_limit = max(10, min(500, chunk_limit))
                req_sig_obj = {
                    "custom_factors": factors,
                    "tail": tail,
                    "params": params,
                    "toggles": toggles,
                    "chunk_offset": chunk_offset,
                    "chunk_limit": chunk_limit,
                    "fields": req_fields,
                    "include_debug": include_debug,
                    "symbols_sig": symbols_sig,
                }
                req_sig = hashlib.sha1(json.dumps(req_sig_obj, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
            except Exception:
                params = payload.get("params") if isinstance(payload.get("params"), dict) else {}
                toggles = payload.get("toggles") if isinstance(payload.get("toggles"), dict) else {}
                fields_raw = payload.get("fields")
                if isinstance(fields_raw, list):
                    req_fields = [str(x).strip() for x in fields_raw if str(x).strip()]
                elif isinstance(fields_raw, str):
                    req_fields = [x.strip() for x in fields_raw.split(",") if x.strip()]
                else:
                    req_fields = []
                include_debug_raw = payload.get("include_debug")
                include_debug = bool(include_debug_raw) if isinstance(include_debug_raw, bool) else str(include_debug_raw or "0").strip().lower() in ("1", "true", "yes", "on")
                symbols_raw = payload.get("symbols")
                symbols_norm = []
                if isinstance(symbols_raw, list):
                    for it in symbols_raw:
                        if isinstance(it, dict):
                            m = str(it.get("market") or "").strip().lower()
                            s = str(it.get("symbol") or "").strip()
                            if m in ("swap", "spot") and s:
                                symbols_norm.append({"market": m, "symbol": s})
                symbols_sig = ""
                try:
                    chunk_offset = max(0, int(payload.get("chunk_offset") or 0))
                except Exception:
                    chunk_offset = 0
                try:
                    chunk_limit = int(payload.get("chunk_limit") or 0)
                except Exception:
                    chunk_limit = 0
                if chunk_limit <= 0:
                    try:
                        chunk_limit = int(os.environ.get("QC_ENRICHED_CHUNK_SIZE") or "120")
                    except Exception:
                        chunk_limit = 120
                chunk_limit = max(10, min(500, chunk_limit))
                req_sig = str(len(factors))

            try:
                st = (repo_root / "apps" / "crypto_screener" / "web" / "data" / "latest.json").stat()
                latest_etag = f"{int(st.st_mtime_ns):x}-{int(st.st_size):x}"
            except Exception:
                latest_etag = "na"
            cache_key = (int(user["id"]), latest_etag, req_sig, int(chunk_offset), int(chunk_limit))
            accept = str(self.headers.get("Accept-Encoding") or "")
            want_gz = "gzip" in accept.lower()

            with enriched_cache_lock:
                hit = enriched_cache.get(cache_key)
                if isinstance(hit, dict):
                    try:
                        ttl0 = int(os.environ.get("QC_ENRICHED_CACHE_TTL_SEC") or "45")
                    except Exception:
                        ttl0 = 45
                    ttl0 = max(5, min(900, ttl0))
                    if time.time() - float(hit.get("ts") or 0.0) > ttl0:
                        enriched_cache.pop(cache_key, None)
                        hit = None
                    else:
                        enriched_cache.move_to_end(cache_key)
            if hit:
                etag = hit.get("etag")
                raw_b = hit.get("raw")
                gz_b = hit.get("gz")
                if self._maybe_304(etag):
                    return
                if want_gz:
                    self._send_bytes(200, gz_b, content_type="application/json; charset=utf-8", etag=etag, content_encoding="gzip")
                else:
                    self._send_bytes(200, raw_b, content_type="application/json; charset=utf-8", etag=etag)
                return
            compiled: list[tuple[str, dict, dict]] = []
            for f in factors:
                if not isinstance(f, dict):
                    continue
                if not (bool(f.get("enabled")) or bool(f.get("show"))):
                    continue
                fid = str(f.get("id") or "").strip()
                if not fid:
                    continue
                template = str(f.get("template") or f.get("expr") or "")
                expr = _expand_template(template, f.get("params") if isinstance(f.get("params"), list) else [])
                try:
                    ast = _compile_expr(expr)
                except Exception:
                    continue
                compiled.append((fid, ast, f))

            rows2 = []
            want_ema = bool(toggles.get("condEma"))
            want_boll_up = bool(toggles.get("condBollUp"))
            want_boll_down = bool(toggles.get("condBollDown"))
            want_super = bool(toggles.get("condSuper"))
            want_kdj = bool(toggles.get("condKdj"))
            want_obv = bool(toggles.get("condObv"))
            want_stoch_rsi = bool(toggles.get("condStochRsi"))

            try:
                ma_close_p = int(params.get("maPeriodClose") or 0)
            except Exception:
                ma_close_p = 0
            try:
                ma_fast_p = int(params.get("maFast") or 0)
            except Exception:
                ma_fast_p = 0
            try:
                ma_slow_p = int(params.get("maSlow") or 0)
            except Exception:
                ma_slow_p = 0
            try:
                rsi_p = int(params.get("rsiPeriod") or 0)
            except Exception:
                rsi_p = 0
            try:
                ema_p = int(params.get("emaPeriod") or 0)
            except Exception:
                ema_p = 0
            try:
                boll_p = int(params.get("bollPeriod") or 0)
            except Exception:
                boll_p = 0
            try:
                boll_std = float(params.get("bollStd") or 0.0)
            except Exception:
                boll_std = 0.0
            try:
                boll_down_p = int(params.get("bollDownPeriod") or 0)
            except Exception:
                boll_down_p = 0
            try:
                boll_down_std = float(params.get("bollDownStd") or 0.0)
            except Exception:
                boll_down_std = 0.0
            try:
                super_atr_p = int(params.get("superAtrPeriod") or 0)
            except Exception:
                super_atr_p = 0
            try:
                super_mult = float(params.get("superMult") or 0.0)
            except Exception:
                super_mult = 0.0
            try:
                kdj_n = int(params.get("kdjN") or 0)
            except Exception:
                kdj_n = 0
            try:
                kdj_m1 = int(params.get("kdjM1") or 0)
            except Exception:
                kdj_m1 = 0
            try:
                kdj_m2 = int(params.get("kdjM2") or 0)
            except Exception:
                kdj_m2 = 0
            try:
                obv_ma_p = int(params.get("obvMaPeriod") or 0)
            except Exception:
                obv_ma_p = 0
            try:
                stoch_rsi_p = int(params.get("stochRsiP") or 0)
            except Exception:
                stoch_rsi_p = 0
            try:
                stoch_rsi_k = int(params.get("stochRsiK") or 0)
            except Exception:
                stoch_rsi_k = 0
            try:
                stoch_rsi_sm_k = int(params.get("stochRsiSmK") or 0)
            except Exception:
                stoch_rsi_sm_k = 0
            try:
                stoch_rsi_sm_d = int(params.get("stochRsiSmD") or 0)
            except Exception:
                stoch_rsi_sm_d = 0

            req_market = str(params.get("market") or "all").strip().lower()
            base_keep = {"market", "symbol", "name", "dt_display", "dt_close", "close", "pct_change", "_rank", "_new_on_list"}
            req_fields_set = set(req_fields)
            return_fields_mode = bool(req_fields_set)
            wanted_builtin_keys = {k for k in req_fields_set if (k.startswith("ma_") or k.startswith("rsi_") or k in {"ema", "boll_up", "boll_down", "supertrend", "kdj_k", "kdj_d", "kdj_j", "obv", "obv_ma", "stoch_rsi_k", "stoch_rsi_d"})}
            wanted_expr_ids = {k[5:] for k in req_fields_set if k.startswith("expr_") and len(k) > 5}
            try:
                budget_ms = int(os.environ.get("QC_ENRICHED_BUDGET_MS") or "25000")
            except Exception:
                budget_ms = 25000
            budget_ms = max(5000, min(55000, budget_ms))
            deadline = time.perf_counter() + (float(budget_ms) / 1000.0)

            row_map: dict[str, dict] = {}
            for rr in all_rows:
                if not isinstance(rr, dict):
                    continue
                mk = str(rr.get("market") or "").strip().lower()
                sk = str(rr.get("symbol") or "").strip()
                if mk in ("swap", "spot") and sk:
                    row_map[f"{mk}|{sk}"] = rr
            use_rows = all_rows
            if symbols_norm:
                subset: list[dict] = []
                seen_keys = set()
                for it in symbols_norm:
                    k = f"{it.get('market')}|{it.get('symbol')}"
                    if k in seen_keys:
                        continue
                    seen_keys.add(k)
                    rr = row_map.get(k)
                    if rr is not None:
                        subset.append(rr)
                use_rows = subset

            total_rows = int(len(use_rows))
            begin = min(max(0, int(chunk_offset)), total_rows)
            end = min(total_rows, begin + int(chunk_limit))
            src_rows = use_rows[begin:end]
            t_series = 0.0
            t_expr = 0.0
            t_builtin = 0.0
            for r in src_rows:
                if time.perf_counter() > deadline and rows2:
                    break
                if not isinstance(r, dict):
                    continue
                sym = str(r.get("symbol") or "").strip()
                market = str(r.get("market") or "").strip().lower()
                if not sym or market not in ("swap", "spot"):
                    rows2.append(r)
                    continue
                if req_market in ("swap", "spot") and market != req_market:
                    rows2.append(r)
                    continue
                t0 = time.perf_counter()
                ctx = _get_series_cached(market=market, symbol=sym, tail=tail)
                t_series += max(0.0, time.perf_counter() - t0)
                if ctx is None:
                    rows2.append(r)
                    continue
                expr_out: dict[str, float | None] = {}
                t1 = time.perf_counter()
                for fid, ast, _f in compiled:
                    if wanted_expr_ids and fid not in wanted_expr_ids:
                        continue
                    try:
                        v = eval_ast(ast, series=ctx["series"], latest=ctx["latest"])
                        expr_out[fid] = _scalar_from_val(v)
                    except Exception:
                        expr_out[fid] = None
                t_expr += max(0.0, time.perf_counter() - t1)
                r2 = dict(r)
                if expr_out:
                    r2["_expr"] = expr_out
                b0 = r.get("_builtins") if isinstance(r.get("_builtins"), dict) else {}
                builtins_out = dict(b0)
                series = ctx.get("series") if isinstance(ctx, dict) else {}
                t2 = time.perf_counter()
                need_closes = bool(ma_close_p > 0 or ma_fast_p > 0 or ma_slow_p > 0 or rsi_p > 0 or want_ema or want_boll_up or want_boll_down or want_super or want_kdj or want_obv or want_stoch_rsi)
                need_hl = bool(want_super or want_kdj)
                need_vol = bool(want_obv)
                if return_fields_mode and wanted_builtin_keys:
                    need_closes = need_closes and bool(wanted_builtin_keys & {f"ma_{ma_close_p}", f"ma_{ma_fast_p}", f"ma_{ma_slow_p}", f"rsi_{rsi_p}", "ema", "boll_up", "boll_down", "supertrend", "kdj_k", "kdj_d", "kdj_j", "obv", "obv_ma", "stoch_rsi_k", "stoch_rsi_d"})
                    need_hl = need_hl and bool(wanted_builtin_keys & {"supertrend", "kdj_k", "kdj_d", "kdj_j"})
                    need_vol = need_vol and bool(wanted_builtin_keys & {"obv", "obv_ma"})
                closes0 = series.get("close") if (need_closes and isinstance(series, dict)) else None
                highs0 = series.get("high") if (need_hl and isinstance(series, dict)) else None
                lows0 = series.get("low") if (need_hl and isinstance(series, dict)) else None
                volumes0 = series.get("volume") if (need_vol and isinstance(series, dict)) else None
                closes = closes0 if isinstance(closes0, list) else _series_to_list(closes0)
                highs = highs0 if isinstance(highs0, list) else _series_to_list(highs0)
                lows = lows0 if isinstance(lows0, list) else _series_to_list(lows0)
                volumes = volumes0 if isinstance(volumes0, list) else _series_to_list(volumes0)

                if ma_close_p > 0 and (not return_fields_mode or f"ma_{ma_close_p}" in wanted_builtin_keys):
                    v0 = sma(closes, ma_close_p)
                    if v0 is not None:
                        builtins_out[f"ma_{ma_close_p}"] = v0
                if ma_fast_p > 0 and (not return_fields_mode or f"ma_{ma_fast_p}" in wanted_builtin_keys):
                    v0 = sma(closes, ma_fast_p)
                    if v0 is not None:
                        builtins_out[f"ma_{ma_fast_p}"] = v0
                if ma_slow_p > 0 and (not return_fields_mode or f"ma_{ma_slow_p}" in wanted_builtin_keys):
                    v0 = sma(closes, ma_slow_p)
                    if v0 is not None:
                        builtins_out[f"ma_{ma_slow_p}"] = v0
                if rsi_p > 0 and (not return_fields_mode or f"rsi_{rsi_p}" in wanted_builtin_keys):
                    v0 = rsi(closes, rsi_p)
                    if v0 is not None:
                        builtins_out[f"rsi_{rsi_p}"] = v0
                if want_ema and ema_p > 0 and (not return_fields_mode or "ema" in wanted_builtin_keys):
                    v0 = ema(closes, ema_p)
                    if v0 is not None:
                        builtins_out["ema"] = v0
                if want_boll_up and boll_p > 0 and (not return_fields_mode or "boll_up" in wanted_builtin_keys):
                    ma_v = sma(closes, boll_p)
                    std_v = rolling_std(closes, boll_p)
                    if ma_v is not None and std_v is not None:
                        builtins_out["boll_up"] = ma_v + boll_std * std_v
                if want_boll_down and boll_down_p > 0 and (not return_fields_mode or "boll_down" in wanted_builtin_keys):
                    ma_v = sma(closes, boll_down_p)
                    std_v = rolling_std(closes, boll_down_p)
                    if ma_v is not None and std_v is not None:
                        builtins_out["boll_down"] = ma_v - boll_down_std * std_v
                if want_super and super_atr_p > 0 and (not return_fields_mode or "supertrend" in wanted_builtin_keys):
                    v0 = supertrend(highs, lows, closes, super_atr_p, super_mult)
                    if v0 is not None:
                        builtins_out["supertrend"] = v0
                if want_kdj and kdj_n > 0 and kdj_m1 > 0 and kdj_m2 > 0 and (not return_fields_mode or bool(wanted_builtin_keys & {"kdj_k", "kdj_d", "kdj_j"})):
                    x = kdj(highs, lows, closes, kdj_n, kdj_m1, kdj_m2)
                    if isinstance(x, dict):
                        k0 = x.get("k")
                        d0 = x.get("d")
                        j0 = x.get("j")
                        if k0 is not None and (not return_fields_mode or "kdj_k" in wanted_builtin_keys):
                            builtins_out["kdj_k"] = k0
                        if d0 is not None and (not return_fields_mode or "kdj_d" in wanted_builtin_keys):
                            builtins_out["kdj_d"] = d0
                        if j0 is not None and (not return_fields_mode or "kdj_j" in wanted_builtin_keys):
                            builtins_out["kdj_j"] = j0
                if want_obv and obv_ma_p > 0 and (not return_fields_mode or bool(wanted_builtin_keys & {"obv", "obv_ma"})):
                    x = obv_with_ma(closes, volumes, obv_ma_p)
                    if isinstance(x, dict):
                        obv0 = x.get("obv")
                        ma0 = x.get("ma")
                        if obv0 is not None and (not return_fields_mode or "obv" in wanted_builtin_keys):
                            builtins_out["obv"] = obv0
                        if ma0 is not None and (not return_fields_mode or "obv_ma" in wanted_builtin_keys):
                            builtins_out["obv_ma"] = ma0
                if want_stoch_rsi and stoch_rsi_p > 0 and stoch_rsi_k > 0 and stoch_rsi_sm_k > 0 and stoch_rsi_sm_d > 0 and (not return_fields_mode or bool(wanted_builtin_keys & {"stoch_rsi_k", "stoch_rsi_d"})):
                    x = stoch_rsi(closes, stoch_rsi_p, stoch_rsi_k, stoch_rsi_sm_k, stoch_rsi_sm_d)
                    if isinstance(x, dict):
                        k0 = x.get("k")
                        d0 = x.get("d")
                        if k0 is not None and (not return_fields_mode or "stoch_rsi_k" in wanted_builtin_keys):
                            builtins_out["stoch_rsi_k"] = k0
                        if d0 is not None and (not return_fields_mode or "stoch_rsi_d" in wanted_builtin_keys):
                            builtins_out["stoch_rsi_d"] = d0
                if builtins_out:
                    r2["_builtins"] = builtins_out
                t_builtin += max(0.0, time.perf_counter() - t2)
                if not include_debug and "_debug" in r2:
                    r2.pop("_debug", None)
                if return_fields_mode:
                    keep = set(base_keep)
                    keep.update(req_fields_set)
                    if "_expr" in r2 and (wanted_expr_ids or not wanted_expr_ids):
                        keep.add("_expr")
                    if "_builtins" in r2 and (wanted_builtin_keys or not wanted_builtin_keys):
                        keep.add("_builtins")
                    r2 = {k: v for k, v in r2.items() if (k in keep)}
                    if "_builtins" in r2 and wanted_builtin_keys:
                        b1 = r2.get("_builtins")
                        if isinstance(b1, dict):
                            r2["_builtins"] = {k: v for k, v in b1.items() if k in wanted_builtin_keys}
                    if "_expr" in r2 and wanted_expr_ids:
                        e1 = r2.get("_expr")
                        if isinstance(e1, dict):
                            r2["_expr"] = {k: v for k, v in e1.items() if k in wanted_expr_ids}
                rows2.append(r2)

            t_encode_start = time.perf_counter()
            resp = {
                "ok": True,
                "summary": latest.get("summary") or {},
                "config": cfg0 or {},
                "results": rows2,
                "chunk": {"offset": int(begin), "limit": int(chunk_limit), "count": int(len(rows2)), "total": int(total_rows)},
            }
            raw = json.dumps(resp, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            gz_b = gzip.compress(raw, compresslevel=6)
            t_encode = max(0.0, time.perf_counter() - t_encode_start)
            etag = f"\"{latest_etag}-{req_sig[:10]}\""
            with enriched_cache_lock:
                enriched_cache[cache_key] = {"ts": time.time(), "etag": etag, "raw": raw, "gz": gz_b}
                enriched_cache.move_to_end(cache_key)
                try:
                    ec_max = int(os.environ.get("QC_ENRICHED_CACHE_MAX") or "800")
                except Exception:
                    ec_max = 800
                ec_max = max(80, min(5000, ec_max))
                while len(enriched_cache) > ec_max:
                    enriched_cache.popitem(last=False)
            with metrics_lock:
                le = metrics.get("latest_enriched")
                if not isinstance(le, dict):
                    le = {"count": 0, "series_s": 0.0, "expr_s": 0.0, "builtins_s": 0.0, "encode_s": 0.0}
                    metrics["latest_enriched"] = le
                le["count"] = int(le.get("count") or 0) + 1
                le["series_s"] = float(le.get("series_s") or 0.0) + float(t_series)
                le["expr_s"] = float(le.get("expr_s") or 0.0) + float(t_expr)
                le["builtins_s"] = float(le.get("builtins_s") or 0.0) + float(t_builtin)
                le["encode_s"] = float(le.get("encode_s") or 0.0) + float(t_encode)
            if self._maybe_304(etag):
                return
            if want_gz:
                self._send_bytes(200, gz_b, content_type="application/json; charset=utf-8", etag=etag, content_encoding="gzip")
            else:
                self._send_bytes(200, raw, content_type="application/json; charset=utf-8", etag=etag)
            return
        if path == "/api/wecom_config":
            user = self._require_auth()
            if not user:
                return
            payload = self._read_json() or {}
            webhook_url = str(payload.get("webhook_url") or "").strip()
            enabled = bool(payload.get("enabled"))
            try:
                top_n = int(payload.get("top_n") or 20)
            except Exception:
                top_n = 20
            config = payload.get("config") if isinstance(payload.get("config"), dict) else {}
            if not webhook_url or not webhook_url.startswith("http"):
                self._send_json(400, {"ok": False, "error": "bad_request", "message": "webhook_url不合法"})
                return
            upsert_wecom_config(auth_cfg, user_id=int(user["id"]), webhook_url=webhook_url, enabled=enabled, top_n=top_n, config=config)
            self._send_json(200, {"ok": True})
            return
        if path == "/api/wecom/send_now":
            user = self._require_auth()
            if not user:
                return
            payload = self._read_json() or {}
            webhook_url = str(payload.get("webhook_url") or "").strip()
            if not webhook_url or not webhook_url.startswith("http"):
                self._send_json(400, {"ok": False, "error": "bad_request", "message": "webhook_url不合法"})
                return
            try:
                top_n = int(payload.get("top_n") or 20)
            except Exception:
                top_n = 20
            top_n = max(1, top_n)
            config = payload.get("config") if isinstance(payload.get("config"), dict) else {}
            latest, all_rows = _load_latest_rows()
            if not all_rows:
                self._send_json(500, {"ok": False, "error": "no_snapshot", "message": "快照不存在，请先运行一次更新/生成快照"})
                return
            tail0 = _pick_series_tail_for_filters(latest=latest, config=config, fallback=720)
            picks0 = payload.get("picks")
            use_picks = isinstance(picks0, list) and bool(picks0)
            if use_picks:
                want_keys: list[str] = []
                seen = set()
                for p in picks0:
                    if not isinstance(p, dict):
                        continue
                    m = str(p.get("market") or "").strip()
                    s = str(p.get("symbol") or "").strip()
                    if not m or not s:
                        continue
                    k = f"{m}|{s}"
                    if k in seen:
                        continue
                    seen.add(k)
                    want_keys.append(k)
                row_map: dict[str, dict] = {}
                for r0 in all_rows:
                    k = f"{str(r0.get('market') or '')}|{str(r0.get('symbol') or '')}"
                    if k in seen:
                        row_map[k] = r0
                subset = [row_map[k] for k in want_keys if k in row_map]
                rows0 = subset
                for i, rr in enumerate(rows0, start=1):
                    rr["_rank"] = i
                sorted_rows = rows0
            else:
                rows0 = _attach_series_to_rows(rows=all_rows, config=config, tail=tail0)
                r = apply_all_filters(rows0, config)
                selected = r.get("selected") if isinstance(r, dict) else []
                selected = selected if isinstance(selected, list) else []
                sorted_rows, sort_key = sort_rows(selected, config)
                assign_rank(sorted_rows, sort_key, config)
            text_ok, text_msg = True, "skipped"
            img_ok = False
            img_msg = "skipped"
            try:
                shown0 = min(top_n, len(sorted_rows))
                rows_img0 = sorted_rows[:shown0]
                rows_img = _attach_series_to_rows(rows=rows_img0, config=config, tail=tail0, force=True)
                img = render_selection_png(title="", rows=rows_img, top_n=shown0)
                img_ok, img_msg = send_image(webhook_url=webhook_url, image_bytes=img)
            except Exception as e:
                img_ok, img_msg = False, str(e)

            count0 = len(sorted_rows)
            shown0 = min(top_n, count0)
            if img_ok and (img_msg or "").strip().lower() == "ok":
                img_msg = f"ok（命中{count0}，展示{shown0}）"
            detail = {"text": {"ok": text_ok, "message": text_msg}, "image": {"ok": img_ok, "message": img_msg, "count": count0, "shown": shown0}}
            if img_ok:
                self._send_json(200, {"ok": True, "detail": detail})
                return
            self._send_json(500, {"ok": False, "error": "send_failed", "message": "发送失败", "detail": detail})
            return
        if path == "/api/email/send_now":
            user = self._require_auth()
            if not user:
                return
            payload = self._read_json() or {}
            to_email = str(payload.get("to_email") or "").strip()
            if not to_email:
                self._send_json(400, {"ok": False, "error": "bad_request", "message": "收件人邮箱不能为空"})
                return
            try:
                top_n = int(payload.get("top_n") or 20)
            except Exception:
                top_n = 20
            top_n = max(1, top_n)
            config = payload.get("config") if isinstance(payload.get("config"), dict) else {}
            latest, all_rows = _load_latest_rows()
            if not all_rows:
                self._send_json(500, {"ok": False, "error": "no_snapshot", "message": "快照不存在，请先运行一次更新/生成快照"})
                return
            tail0 = _pick_series_tail_for_filters(latest=latest, config=config, fallback=720)
            rows0 = _attach_series_to_rows(rows=all_rows, config=config, tail=tail0)
            r = apply_all_filters(rows0, config)
            selected = r.get("selected") if isinstance(r, dict) else []
            selected = selected if isinstance(selected, list) else []
            sorted_rows, sort_key = sort_rows(selected, config)
            assign_rank(sorted_rows, sort_key, config)
            content = _build_wecom_markdown(latest=latest, rows=sorted_rows, top_n=top_n)
            summary = (latest or {}).get("summary") or {}
            dt = str(summary.get("latest_dt_display") or summary.get("latest_dt_close") or summary.get("generated_at") or "")
            subject = f"自动化选币器｜{dt}"
            ok, msg = _send_email(to_email=to_email, subject=subject, body=content)
            if ok:
                self._send_json(200, {"ok": True})
                return
            self._send_json(500, {"ok": False, "error": "email_send_failed", "message": msg})
            return
        if path == "/api/custom_factors/replace":
            user = self._require_auth()
            if not user:
                return
            payload = self._read_json() or {}
            factors = payload.get("factors")
            ok, msg = replace_custom_factors(auth_cfg, user_id=int(user["id"]), factors=factors if isinstance(factors, list) else [])
            if not ok:
                self._send_json(400, {"ok": False, "error": "bad_request", "message": msg})
                return
            self._send_json(200, {"ok": True})
            return
        if path == "/api/custom_factors/upsert":
            user = self._require_auth()
            if not user:
                return
            payload = self._read_json() or {}
            factor = payload.get("factor") if isinstance(payload.get("factor"), dict) else payload
            ok, msg = upsert_custom_factor(auth_cfg, user_id=int(user["id"]), factor=factor if isinstance(factor, dict) else {})
            if not ok:
                self._send_json(400, {"ok": False, "error": "bad_request", "message": msg})
                return
            self._send_json(200, {"ok": True})
            return
        if path == "/api/custom_factors/delete":
            user = self._require_auth()
            if not user:
                return
            payload = self._read_json() or {}
            fid = str(payload.get("id") or payload.get("factor_id") or "").strip()
            if not fid:
                self._send_json(400, {"ok": False, "error": "bad_request"})
                return
            delete_custom_factor(auth_cfg, user_id=int(user["id"]), factor_id=fid)
            self._send_json(200, {"ok": True})
            return
        if path == "/api/feedback":
            user = self._require_auth()
            if not user:
                return
            payload = self._read_json() or {}
            message = str(payload.get("message") or "").strip()
            if not message:
                self._send_json(400, {"ok": False, "error": "bad_request", "message": "反馈内容不能为空"})
                return
            missing = _smtp_missing_env()
            if missing:
                self._send_json(500, {"ok": False, "error": "email_not_configured", "missing": missing})
                return
            from_email = str(user.get("email") or "")
            from_username = str(user.get("username") or "")
            subject = f"自动化选币器反馈 - {from_email or from_username or 'unknown'}"
            body = f"from_email: {from_email}\nfrom_username: {from_username}\n\n{message}\n"
            ok, msg = _send_email(to_email="grailweb3@163.com", subject=subject, body=body)
            if not ok:
                self._send_json(500, {"ok": False, "error": "email_send_failed", "message": msg})
                return
            self._send_json(200, {"ok": True})
            return
        if path == "/api/send_code":
            payload = self._read_json() or {}
            email_raw = str(payload.get("email") or "").strip()
            email = _normalize_email_addr(email_raw)
            phone = str(payload.get("phone") or "").strip()
            scene = str(payload.get("scene") or "").strip().lower()
            if email_raw and not email:
                self._send_json(400, {"ok": False, "error": "bad_request", "message": "邮箱格式不合法"})
                return
            if email:
                ok, msg, code = create_email_code(auth_cfg, email=email, scene=scene)
                if not ok or not code:
                    self._send_json(400, {"ok": False, "error": "bad_request", "message": msg})
                    return
                missing = _smtp_missing_env()
                if missing:
                    self._send_json(500, {"ok": False, "error": "email_not_configured", "missing": missing})
                    return
                sent, emsg = _send_email_code(to_email=email, scene=scene, code=code)
                if not sent:
                    self._send_json(500, {"ok": False, "error": "email_send_failed", "message": emsg})
                    return
                self._send_json(200, {"ok": True})
                return
            ok, msg, code = create_sms_code(auth_cfg, phone=phone, scene=scene)
            if not ok or not code:
                self._send_json(400, {"ok": False, "error": "bad_request", "message": msg})
                return
            sms_cfg = load_aliyun_sms()
            if sms_cfg is None:
                self._send_json(500, {"ok": False, "error": "sms_not_configured", "missing": self._sms_missing_env()})
                return
            sent, smsg = aliyun_send_code(cfg=sms_cfg, phone=phone, scene=scene, code=code)
            if not sent:
                self._send_json(500, {"ok": False, "error": "sms_send_failed", "message": smsg})
                return
            self._send_json(200, {"ok": True})
            return
        if path == "/api/register":
            payload = self._read_json() or {}
            username = str(payload.get("username") or "").strip()
            email_raw = str(payload.get("email") or "").strip()
            email = _normalize_email_addr(email_raw)
            phone = str(payload.get("phone") or "").strip()
            password = str(payload.get("password") or "")
            password2 = str(payload.get("password2") or "")
            code = str(payload.get("code") or "").strip()
            if email_raw and not email:
                self._send_json(400, {"ok": False, "error": "bad_request", "message": "邮箱格式不合法"})
                return
            if password != password2:
                self._send_json(400, {"ok": False, "error": "bad_request", "message": "两次密码不一致"})
                return
            if not verify_email_code(auth_cfg, email=email, scene="register", code=code):
                self._send_json(400, {"ok": False, "error": "bad_request", "message": "验证码错误或已过期"})
                return
            ok, msg = register_user(auth_cfg, username=username, email=email, phone=phone or None, password=password)
            if not ok:
                self._send_json(400, {"ok": False, "error": "bad_request", "message": msg})
                return
            self._send_json(200, {"ok": True})
            return
        if path == "/api/login":
            payload = self._read_json() or {}
            mode = str(payload.get("mode") or "").strip().lower()
            if mode == "code":
                email_raw = str(payload.get("email") or "").strip()
                email = _normalize_email_addr(email_raw)
                code = str(payload.get("code") or "").strip()
                if email_raw and not email:
                    self._send_json(400, {"ok": False, "error": "bad_request", "message": "邮箱格式不合法"})
                    return
                if not verify_email_code(auth_cfg, email=email, scene="login", code=code):
                    self._send_json(401, {"ok": False, "error": "bad_code"})
                    return
                u = get_user_by_username_or_phone(auth_cfg, identity=email)
                if not u or not u.get("id"):
                    self._send_json(404, {"ok": False, "error": "user_not_found"})
                    return
                uid = int(u["id"])
            else:
                identity = str(payload.get("identity") or payload.get("username") or "").strip()
                password = str(payload.get("password") or "")
                ok, uid = verify_user_identity_password(auth_cfg, identity=identity, password=password)
                if not ok or uid is None:
                    self._send_json(401, {"ok": False, "error": "bad_credentials"})
                    return
            sid = create_session(auth_cfg, user_id=int(uid))
            self.send_response(200)
            self._set_cookie(AUTH_COOKIE, sid, max_age=auth_cfg.session_ttl_days * 86400)
            body = json.dumps({"ok": True}, ensure_ascii=False).encode("utf-8")
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if path == "/api/logout":
            sid = self._get_session_id()
            if sid:
                delete_session(auth_cfg, session_id=sid)
            self.send_response(200)
            self._set_cookie(AUTH_COOKIE, "deleted", max_age=0)
            body = json.dumps({"ok": True}, ensure_ascii=False).encode("utf-8")
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if path != "/api/refresh":
            self._send_json(404, {"ok": False, "error": "not_found"})
            return
        if not self._require_auth():
            return
        qs = parse_qs(parsed.query or "")
        fetch_raw = (qs.get("fetch", ["1"])[0] or "1").strip()
        fetch = fetch_raw not in ("0", "false", "no")
        if not start_update(fetch=fetch):
            self._send_json(409, {"ok": False, "error": "busy"})
            return
        self._send_json(202, {"ok": True, "started": True, "fetch": fetch})

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path or ""
        if path != "/":
            path = path.rstrip("/")
        if path == "":
            path = "/"
        if path == "/api/public_config":
            prefix = (os.environ.get("QC_ALIYUN_CAPTCHA_PREFIX") or os.environ.get("QC_ALIYUN_SMS_PREFIX") or "").strip()
            scene_id = (os.environ.get("QC_ALIYUN_CAPTCHA_SCENE_ID") or "wfh1k2qh").strip()
            region = (os.environ.get("QC_ALIYUN_CAPTCHA_REGION") or "cn").strip()
            enabled = bool(prefix and scene_id)
            self._send_json(200, {"ok": True, "captcha": {"enabled": enabled, "prefix": prefix, "sceneId": scene_id, "region": region}})
            return
        if path == "/api/check_username":
            qs = parse_qs(parsed.query or "")
            username = (qs.get("username", [""])[0] or "").strip()
            valid = _username_ok_public(username)
            exists = False
            if valid:
                try:
                    u = get_user_by_username_or_phone(auth_cfg, identity=username)
                    exists = bool(u and str(u.get("username") or "") == username)
                except Exception:
                    exists = False
            self._send_json(200, {"ok": True, "username": username, "valid": valid, "exists": exists})
            return
        if path == "/api/debug_env":
            missing = _sms_missing_env()
            self._send_json(
                200,
                {
                    "ok": True,
                    "server": SERVER_INFO,
                    "sms": {
                        "enabled": not bool(missing),
                        "missing": missing,
                        "has_prefix": bool(
                            (os.environ.get("QC_ALIYUN_SMS_PREFIX") or os.environ.get("QC_ALIYUN_ACCESS_KEY_ID") or os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_ID") or "").strip()
                        ),
                        "has_ekey": bool(
                            (os.environ.get("QC_ALIYUN_SMS_EKEY") or os.environ.get("QC_ALIYUN_ACCESS_KEY_SECRET") or os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_SECRET") or "").strip()
                        ),
                        "has_sign_name": bool((os.environ.get("QC_ALIYUN_SMS_SIGN_NAME") or "").strip()),
                        "has_template_register": bool((os.environ.get("QC_ALIYUN_SMS_TEMPLATE_REGISTER") or "").strip()),
                        "has_template_login": bool((os.environ.get("QC_ALIYUN_SMS_TEMPLATE_LOGIN") or "").strip()),
                    },
                    "email": {
                        "enabled": not bool(_smtp_missing_env()),
                        "missing": _smtp_missing_env(),
                        "has_host": bool((os.environ.get("QC_SMTP_HOST") or "").strip()),
                        "has_user": bool((os.environ.get("QC_SMTP_USER") or "").strip()),
                        "has_pass": bool((os.environ.get("QC_SMTP_PASS") or "").strip()),
                    },
                },
            )
            return
        if path == "/api/me":
            user = self._current_user()
            if not user:
                self._send_json(200, {"ok": True, "user": None})
                return
            self._send_json(200, {"ok": True, "user": {"id": user["id"], "username": user["username"], "email": user.get("email")}})
            return
        if path == "/api/custom_factors":
            user = self._require_auth()
            if not user:
                return
            fs = list_custom_factors(auth_cfg, user_id=int(user["id"]))
            self._send_json(200, {"ok": True, "factors": fs})
            return
        if path == "/api/wecom_config":
            user = self._require_auth()
            if not user:
                return
            cfg0 = get_wecom_config(auth_cfg, user_id=int(user["id"]))
            self._send_json(200, {"ok": True, "config": cfg0})
            return
        if path == "/api/echo":
            if not self._require_auth():
                return
            self._send_json(200, {"ok": True, "raw": self.path, "path": parsed.path, "query": parsed.query})
            return
        if path == "/api/status":
            if not self._require_auth():
                return
            paths = default_paths()
            lock_info = None
            try:
                if paths.lock_file.exists():
                    lock_info = {
                        "path": str(paths.lock_file),
                        "mtime": datetime.fromtimestamp(paths.lock_file.stat().st_mtime).isoformat(timespec="seconds"),
                        "pid": (paths.lock_file.read_text(encoding="utf-8", errors="ignore") or "").strip() or None,
                    }
            except Exception:
                lock_info = None
            with pkl_build_lock:
                pkl0 = json.loads(json.dumps(pkl_build_state))
            self._send_json(200, {"ok": True, "server": "v2", "req_raw": self.path, "req_path": parsed.path, "update": update_state, "pkl": pkl0, "lock": lock_info})
            return
        if path == "/api/metrics":
            if not self._require_auth():
                return
            with metrics_lock:
                m = json.loads(json.dumps(metrics))
            try:
                from apps.crypto_screener.app import filter_engine as fe  # noqa: E402

                m["factor_cache"] = fe.get_factor_cache_metrics()
            except Exception:
                pass
            extra = {
                "cache": {
                    "latest_cache": 1 if latest_cache else 0,
                    "expr_cache": len(expr_cache),
                    "series_cache": len(series_cache),
                    "enriched_cache": len(enriched_cache),
                    "gz_cache": len(gz_cache),
                }
            }
            self._send_json(200, {"ok": True, "metrics": m, "extra": extra})
            return
        if path == "/api/debug/kline_source":
            if not self._require_auth():
                return
            qs = parse_qs(parsed.query or "")
            market = (qs.get("market", [""])[0] or "").strip().lower()
            symbol = (qs.get("symbol", [""])[0] or "").strip()
            tail_raw = (qs.get("tail", ["720"])[0] or "720").strip()
            try:
                tail = int(tail_raw)
            except Exception:
                tail = 720
            tail = max(30, min(3650, tail))
            if market not in ("swap", "spot"):
                self._send_json(400, {"ok": False, "error": "bad_market"})
                return
            sym = symbol.upper()
            if not sym.endswith("-USDT") or len(sym) > 32:
                self._send_json(400, {"ok": False, "error": "bad_symbol"})
                return
            for ch in sym:
                if not (ch.isalnum() or ch in "-_"):
                    self._send_json(400, {"ok": False, "error": "bad_symbol"})
                    return
            key = (str(market).lower(), str(sym), int(tail))
            cache_info = {"present": False, "dt_len": None}
            with series_cache_lock:
                hit0 = series_cache.get(key)
            if hit0 is not None:
                cache_info["present"] = True
                payload0 = hit0.get("payload") if isinstance(hit0, dict) and "payload" in hit0 else hit0
                try:
                    dt0 = payload0.get("dt") if isinstance(payload0, dict) else None
                    cache_info["dt_len"] = len(dt0) if isinstance(dt0, list) else 0
                except Exception:
                    cache_info["dt_len"] = None
            csv_info = {"merge_dir": None, "dc_dir": None, "picked_from": None, "picked_sym": None, "csv_path": None, "lines_est": None, "size_bytes": None}
            try:
                from apps.crypto_screener.app import series_source as ss  # noqa: E402

                swap_dir, spot_dir = ss._default_merge_dirs(repo_root)
                dc_swap_dir, dc_spot_dir = ss._default_data_center_dirs(repo_root)
                base_merge = swap_dir if market == "swap" else spot_dir
                base_dc = dc_swap_dir if market == "swap" else dc_spot_dir
                csv_info["merge_dir"] = str(base_merge)
                csv_info["dc_dir"] = str(base_dc)
                p0, picked_sym = ss._pick_existing_csv([base_merge, base_dc], sym, tail_hint=tail)
                if p0 is not None:
                    csv_info["picked_sym"] = picked_sym
                    csv_info["csv_path"] = str(p0)
                    try:
                        st = p0.stat()
                        csv_info["size_bytes"] = int(st.st_size)
                    except Exception:
                        pass
                    try:
                        n = 0
                        with p0.open("rb") as f:
                            while True:
                                buf = f.read(1024 * 1024)
                                if not buf:
                                    break
                                n += int(buf.count(b"\n"))
                        csv_info["lines_est"] = int(n)
                    except Exception:
                        pass
                    try:
                        pr = p0.resolve()
                        if base_merge.resolve() in pr.parents or pr == base_merge.resolve():
                            csv_info["picked_from"] = "merge"
                        elif base_dc.resolve() in pr.parents or pr == base_dc.resolve():
                            csv_info["picked_from"] = "data_center"
                    except Exception:
                        pass
            except Exception:
                pass
            ctx_dt_len = None
            ctx_err = None
            try:
                ctx = _get_series_cached(market=market, symbol=sym, tail=tail)
            except Exception as e:
                ctx = None
                ctx_err = str(e)
            if isinstance(ctx, dict):
                try:
                    dt0 = ctx.get("dt") or []
                    ctx_dt_len = len(dt0) if isinstance(dt0, list) else 0
                except Exception:
                    ctx_dt_len = None
            env0 = {
                "QC_MERGE_SWAP_PATH": os.environ.get("QC_MERGE_SWAP_PATH"),
                "QC_MERGE_SPOT_PATH": os.environ.get("QC_MERGE_SPOT_PATH"),
                "QC_SCREENER_FALLBACK_SWAP_DIR": os.environ.get("QC_SCREENER_FALLBACK_SWAP_DIR"),
                "QC_SCREENER_FALLBACK_SPOT_DIR": os.environ.get("QC_SCREENER_FALLBACK_SPOT_DIR"),
                "QC_DATA_CENTER_ROOT": os.environ.get("QC_DATA_CENTER_ROOT"),
                "QC_USE_PKL_SERIES_CACHE": os.environ.get("QC_USE_PKL_SERIES_CACHE"),
                "QC_USE_PREPROCESSED_SERIES": os.environ.get("QC_USE_PREPROCESSED_SERIES"),
                "QC_PKL_CACHE_ROOT": os.environ.get("QC_PKL_CACHE_ROOT"),
                "QC_BUILD_PKL_CACHE": os.environ.get("QC_BUILD_PKL_CACHE"),
            }
            ctx_src = ctx.get("source") if isinstance(ctx, dict) else None
            pkl0 = {"series": {"present": False, "dt_len": None}, "factors": {"present": False}}
            try:
                from apps.crypto_screener.app import series_source as ss  # noqa: E402

                s_pkl, _stale = ss._load_series_from_pkl_cache(market=market, symbol=sym, tail=tail, repo_root=repo_root, return_stale=True)
                if s_pkl is not None:
                    pkl0["series"]["present"] = True
                    pkl0["series"]["dt_len"] = len(getattr(s_pkl, "dt", []) or [])
            except Exception:
                pass
            try:
                from apps.crypto_screener.app import filter_engine as fe  # noqa: E402

                f_pkl = fe._get_cached_factors(market=market, symbol=sym)
                pkl0["factors"]["present"] = bool(isinstance(f_pkl, dict) and f_pkl)
            except Exception:
                pass
            self._send_json(
                200,
                {
                    "ok": True,
                    "repo_root": str(repo_root),
                    "market": market,
                    "symbol": sym,
                    "tail_req": int(tail),
                    "env": env0,
                    "series_cache": cache_info,
                    "csv": csv_info,
                    "pkl_cache": pkl0,
                    "ctx": {"dt_len": ctx_dt_len, "source": ctx_src, "error": ctx_err},
                },
            )
            return
        if path == "/api/kline":
            if not self._require_auth():
                return
            qs = parse_qs(parsed.query or "")
            market = (qs.get("market", [""])[0] or "").strip().lower()
            symbol = (qs.get("symbol", [""])[0] or "").strip()
            tail_raw = (qs.get("tail", ["360"])[0] or "360").strip()
            try:
                tail = int(tail_raw)
            except Exception:
                tail = 360
            tail = max(30, min(3650, tail))

            if market not in ("swap", "spot"):
                self._send_json(400, {"ok": False, "error": "bad_market"})
                return
            sym = symbol.upper()
            if not sym.endswith("-USDT") or len(sym) > 32:
                self._send_json(400, {"ok": False, "error": "bad_symbol"})
                return
            for ch in sym:
                if not (ch.isalnum() or ch in "-_"):
                    self._send_json(400, {"ok": False, "error": "bad_symbol"})
                    return

            try:
                ctx = _get_series_cached(market=market, symbol=sym, tail=tail)
            except Exception:
                ctx = None
            if ctx is not None:
                s0 = ctx.get("series") if isinstance(ctx, dict) else None
                dt0 = ctx.get("dt") or []
                out = {"ok": True, "market": market, "symbol": sym, "tail": 0, "bar_hours": 1, "dt": (dt0 if isinstance(dt0, list) else []), "series": {}}
                try:
                    import numpy as np  # noqa: E402
                except Exception:
                    np = None
                for col in ("open", "high", "low", "close", "volume", "quote_volume"):
                    arr = (s0 or {}).get(col) if isinstance(s0, dict) else None
                    if arr is None:
                        continue
                    if np is not None and hasattr(arr, "shape"):
                        out["series"][col] = [None if float(v) != float(v) else float(v) for v in arr.tolist()]
                    else:
                        out["series"][col] = [None if v is None else float(v) for v in list(arr)]
                try:
                    out["tail"] = int(len(out.get("dt") or []))
                except Exception:
                    out["tail"] = 0
                self._send_json(200, out)
                return

            try:
                import pandas as pd  # noqa: E402
                from apps.crypto_screener.app.screener import read_symbol_csv_tail  # noqa: E402
            except Exception:
                self._send_json(500, {"ok": False, "error": "server_missing_deps"})
                return

            if os.name == "nt":
                _swap0 = r"D:\量化交易\数据\swap_lin"
                _spot0 = r"D:\量化交易\数据\spot_lin"
            else:
                _swap0 = str(repo_root / "数据获取" / "data" / "swap_lin")
                _spot0 = str(repo_root / "数据获取" / "data" / "spot_lin")
            swap_dir = Path(os.environ.get("QC_SCREENER_FALLBACK_SWAP_DIR") or _swap0)
            spot_dir = Path(os.environ.get("QC_SCREENER_FALLBACK_SPOT_DIR") or _spot0)
            base_dir = swap_dir if market == "swap" else spot_dir
            sym_cands = [sym]
            if sym.endswith("-USDT"):
                sym_cands.append(sym.replace("-", ""))
            elif sym.endswith("USDT"):
                sym_cands.append(f"{sym[:-4]}-USDT")
            csv_path = None
            for s1 in sym_cands:
                p = (base_dir / f"{s1}.csv").resolve()
                if base_dir not in p.parents and p != base_dir:
                    self._send_json(403, {"ok": False, "error": "forbidden"})
                    return
                if p.exists() and p.is_file():
                    csv_path = p
                    break
            if csv_path is None:
                self._send_json(404, {"ok": False, "error": "not_found", "symbol": sym, "base_dir": str(base_dir)})
                return

            try:
                df = read_symbol_csv_tail(csv_path, max_lines=tail + 300)
                if df is None or df.empty:
                    self._send_json(404, {"ok": False, "error": "empty"})
                    return
                try:
                    df.columns = [str(c).lstrip("\ufeff").strip() for c in list(df.columns)]
                except Exception:
                    pass
                if "candle_begin_time" not in df.columns:
                    self._send_json(500, {"ok": False, "error": "bad_csv"})
                    return
                s = df["candle_begin_time"]
                dt = pd.to_datetime(s, errors="coerce", utc=True, format="mixed")
                try:
                    ok_cnt = int(dt.notna().sum())
                    n_cnt = int(len(dt))
                except Exception:
                    ok_cnt = 0
                    n_cnt = 0
                if n_cnt > 0 and ok_cnt < max(3, int(n_cnt * 0.2)):
                    xs = pd.to_numeric(s, errors="coerce")
                    try:
                        x_ok = int(xs.notna().sum())
                    except Exception:
                        x_ok = 0
                    if x_ok >= max(3, int(n_cnt * 0.8)):
                        dt_ms = pd.to_datetime(xs, errors="coerce", utc=True, unit="ms")
                        dt_s = pd.to_datetime(xs, errors="coerce", utc=True, unit="s")
                        if int(dt_ms.notna().sum()) >= ok_cnt:
                            dt = dt_ms
                            ok_cnt = int(dt.notna().sum())
                        if int(dt_s.notna().sum()) > ok_cnt:
                            dt = dt_s
                df["candle_begin_time"] = dt
                df = df.dropna(subset=["candle_begin_time"]).sort_values("candle_begin_time").tail(tail)
                if df.empty:
                    self._send_json(404, {"ok": False, "error": "empty"})
                    return
                out = {
                    "ok": True,
                    "market": market,
                    "symbol": sym,
                    "tail": int(len(df)),
                    "bar_hours": 1,
                    "dt": [x.isoformat() for x in df["candle_begin_time"].tolist()],
                    "series": {},
                }
                for col in ("open", "high", "low", "close", "volume", "quote_volume"):
                    if col in df.columns:
                        out["series"][col] = [None if pd.isna(v) else float(v) for v in pd.to_numeric(df[col], errors="coerce").tolist()]
                self._send_json(200, out)
                return
            except Exception as e:
                self._send_json(500, {"ok": False, "error": "internal", "message": str(e)})
                return
        user = self._current_user()
        public_static = {"/login.html", "/style.css", "/grailweb3.png", "/user_agreement.html", "/privacy.html", "/vendor/aliyunCaptcha.js"}
        if not user:
            if path in ("", "/"):
                self._redirect("/login.html?next=%2F")
                return
            if path == "/index.html":
                self._redirect("/login.html?next=%2F")
                return
            if path.endswith(".html") and path != "/login.html" and path not in public_static:
                self._redirect("/login.html?next=" + quote(self.path, safe=""))
                return
            if path.startswith("/data/") or path.startswith("/api/"):
                self._send_json(401, {"ok": False, "error": "unauthorized"})
                return
            if path not in public_static:
                self._send_json(401, {"ok": False, "error": "unauthorized"})
                return

        if path == "/":
            self._redirect("/index.html")
            return
        else:
            rel = path.lstrip("/")
        base = repo_root / "apps" / "crypto_screener" / "web"
        target = (base / rel).resolve()
        if base not in target.parents and target != base:
            self.send_error(403)
            return
        if not target.exists() or not target.is_file():
            self.send_error(404)
            return
        ctype = "application/octet-stream"
        suffix = target.suffix.lower()
        if suffix in (".html",):
            ctype = "text/html; charset=utf-8"
        elif suffix in (".css",):
            ctype = "text/css; charset=utf-8"
        elif suffix in (".js",):
            ctype = "application/javascript; charset=utf-8"
        elif suffix in (".json",):
            ctype = "application/json; charset=utf-8"
        elif suffix in (".md",):
            ctype = "text/plain; charset=utf-8"
        elif suffix in (".png",):
            ctype = "image/png"
        elif suffix in (".jpg", ".jpeg"):
            ctype = "image/jpeg"

        accept = str(self.headers.get("Accept-Encoding") or "")
        use_gzip = "gzip" in accept.lower()
        can_gzip = suffix in (".html", ".css", ".js", ".json", ".md")
        gz_target = None
        if use_gzip and can_gzip:
            gz_target = (target.parent / (target.name + ".gz")).resolve()
            if not (base in gz_target.parents or gz_target == base):
                gz_target = None
            if gz_target and (not gz_target.exists() or not gz_target.is_file()):
                gz_target = None

        chosen = gz_target or target
        if gz_target:
            etag = self._etag_from_file(gz_target)
            if self._maybe_304(etag):
                return
            data = gz_target.read_bytes()
            self._send_bytes(200, data, content_type=ctype, etag=etag, content_encoding="gzip")
            return

        if use_gzip and can_gzip:
            etag0 = self._etag_from_file(target)
            etag = f"{etag0[:-1]}-gz\""
            if self._maybe_304(etag):
                return
            key = str(target)
            with gz_cache_lock:
                hit = gz_cache.get(key)
            if hit and hit[0] == etag:
                self._send_bytes(200, hit[1], content_type=ctype, etag=etag, content_encoding="gzip")
                return
            raw = target.read_bytes()
            buf = BytesIO()
            with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=6, mtime=0) as f:
                f.write(raw)
            gz = buf.getvalue()
            with gz_cache_lock:
                gz_cache[key] = (etag, gz)
            self._send_bytes(200, gz, content_type=ctype, etag=etag, content_encoding="gzip")
            return

        etag = self._etag_from_file(target)
        if self._maybe_304(etag):
            return
        data = target.read_bytes()
        self._send_bytes(200, data, content_type=ctype, etag=etag)
        return


def main() -> None:
    init_db(auth_cfg)
    cleanup_sessions(auth_cfg)
    cleanup_sms(auth_cfg)
    cleanup_email(auth_cfg)
    t = threading.Thread(target=_scheduler_loop, daemon=True)
    t.start()
    if os.environ.get("QC_BOOTSTRAP_UPDATE") == "1":
        now = datetime.now()
        latest_path = repo_root / "apps" / "crypto_screener" / "web" / "data" / "latest.json"
        if not latest_path.exists():
            start_update(fetch=False)
        elif now.minute == 0:
            start_update(fetch=True)

    missing = _sms_missing_env()
    if missing:
        print("Aliyun SMS not configured, missing: " + ", ".join(missing), flush=True)
    else:
        print("Aliyun SMS configured", flush=True)

    host = os.environ.get("QC_SCREENER_HOST") or "127.0.0.1"
    base_port = int(os.environ.get("QC_SCREENER_PORT") or "8001")
    strict_port = os.environ.get("QC_SCREENER_PORT_STRICT") == "1"
    server = None
    port_range = range(base_port, base_port + 1) if strict_port else range(base_port, base_port + 10)
    for port in port_range:
        try:
            server = ThreadingHTTPServer((host, port), Handler)
            print(f"crypto_screener web_server started (v2): http://{host}:{port}/", flush=True)
            break
        except OSError:
            continue
    if server is None:
        raise RuntimeError(f"无法绑定端口：{host}:{base_port}~{base_port+9}")
    server.serve_forever()


if __name__ == "__main__":
    main()
