"""backtest/worker.py — 异步任务队列管理

任务状态: pending → running → done / failed / cancelled
每用户最多3个并发任务，结果保留1小时后自动清理。
"""
from __future__ import annotations

import sys
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

_repo_root = Path(__file__).resolve().parents[3]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from apps.crypto_screener.backtest.engine import (
    BacktestConfig,
    BacktestResult,
    run_backtest,
)


MAX_TASKS_PER_USER = 3
TASK_TTL_SECONDS = 3600   # 结果保留1小时
MAX_TIME_RANGE_DAYS = 90  # Pro用户最大回测时长
MAX_TOP_N = 50


@dataclass
class BacktestTask:
    task_id: str
    user: str
    config: dict
    status: str = 'pending'   # pending / running / done / failed / cancelled
    progress: int = 0         # 0-100
    message: str = ''
    result: Any = None        # BacktestResult
    error: str = ''
    created_at: float = field(default_factory=time.time)
    finished_at: float = 0.0


_tasks: dict[str, BacktestTask] = {}
_tasks_lock = threading.Lock()


def _cleanup_old_tasks() -> None:
    """清理过期任务（需在持锁状态下调用）"""
    now = time.time()
    to_del = [
        tid for tid, t in _tasks.items()
        if t.status in ('done', 'failed', 'cancelled')
        and now - t.finished_at > TASK_TTL_SECONDS
    ]
    for tid in to_del:
        _tasks.pop(tid, None)


def _validate_config(config: dict) -> BacktestConfig:
    """校验并构建 BacktestConfig，拒绝非法字段"""
    ALLOWED_PARAM_KEYS = {
        'market', 'maPeriodClose', 'maFast', 'maSlow',
        'rsiPeriod', 'rsiThreshold', 'emaPeriod',
        'bollPeriod', 'bollStd', 'bollDownPeriod', 'bollDownStd',
        'superAtrPeriod', 'superMult',
        'kdjN', 'kdjM1', 'kdjM2',
        'obvMaPeriod',
        'stochRsiP', 'stochRsiK', 'stochRsiSmK', 'stochRsiSmD',
    }
    ALLOWED_TOGGLE_KEYS = {
        'condCloseMa', 'condMa', 'condRsi', 'condEma',
        'condBollUp', 'condBollDown', 'condSuper',
        'condKdj', 'condObv', 'condStochRsi',
    }

    raw_params = config.get('params') or {}
    raw_toggles = config.get('toggles') or {}

    params = {k: v for k, v in raw_params.items() if k in ALLOWED_PARAM_KEYS}
    toggles = {k: bool(v) for k, v in raw_toggles.items() if k in ALLOWED_TOGGLE_KEYS}

    # 自定义因子：只保留已知字段
    raw_factors = config.get('custom_factors') or []
    custom_factors = []
    if isinstance(raw_factors, list):
        for f in raw_factors:
            if isinstance(f, dict):
                custom_factors.append({
                    'id': str(f.get('id') or ''),
                    'template': str(f.get('template') or f.get('expr') or ''),
                    'params': f.get('params') or [],
                    'enabled': bool(f.get('enabled', True)),
                    'thresholdEnabled': bool(f.get('thresholdEnabled', False)),
                    'cmp': str(f.get('cmp') or '>='),
                    'threshold': float(f.get('threshold') or 0.0),
                })

    market = str(config.get('market') or 'swap').lower()
    if market not in ('swap', 'spot', 'all'):
        market = 'swap'

    hold_hours = max(1, min(168, int(config.get('hold_hours') or 1)))
    tail_len = max(60, min(360, int(config.get('tail_len') or 200)))
    top_n = max(0, min(MAX_TOP_N, int(config.get('top_n') or 0)))
    fee_rate = max(0.0, min(0.01, float(config.get('fee_rate') or 0.0005)))
    leverage = max(0.1, min(20.0, float(config.get('leverage') or 1.0)))
    direction = str(config.get('direction') or 'long').lower()
    if direction not in ('long', 'short'):
        direction = 'long'

    # 现货（spot）和全市场（all含现货）不支持做空
    if direction == 'short' and market in ('spot', 'all'):
        raise ValueError('现货市场不支持做空，请选择合约市场或将方向改为做多')

    start_dt = str(config.get('start_dt') or '')
    end_dt = str(config.get('end_dt') or '')
    if not start_dt or not end_dt:
        raise ValueError('start_dt 和 end_dt 不能为空')

    # 排序配置
    ALLOWED_SORT_KEYS = {
        'pct_change', 'close', 'volume', 'quote_volume',
        'ema', 'boll_up', 'boll_down', 'supertrend',
        'kdj_k', 'kdj_d', 'obv', 'stoch_rsi_k',
    }
    sort_key = str(config.get('sort_key') or 'pct_change')
    # 允许 ma_N 和 rsi_N 格式
    import re as _re
    if sort_key not in ALLOWED_SORT_KEYS and not _re.match(r'^(ma|rsi)_\d+$', sort_key) and not sort_key.startswith('expr_'):
        sort_key = 'pct_change'
    sort_order = str(config.get('sort_order') or 'desc').lower()
    if sort_order not in ('asc', 'desc'):
        sort_order = 'desc'

    full_coverage = bool(config.get('full_coverage', False))

    whitelist = [str(s) for s in (config.get('whitelist') or []) if s]
    blacklist = [str(s) for s in (config.get('blacklist') or []) if s]

    return BacktestConfig(
        params=params,
        toggles=toggles,
        custom_factors=custom_factors,
        market=market,
        start_dt=start_dt,
        end_dt=end_dt,
        hold_hours=hold_hours,
        tail_len=tail_len,
        top_n=top_n,
        fee_rate=fee_rate,
        leverage=leverage,
        direction=direction,
        sort_key=sort_key,
        sort_order=sort_order,
        full_coverage=full_coverage,
        whitelist=whitelist,
        blacklist=blacklist,
    )


def _run_task(task_id: str) -> None:
    with _tasks_lock:
        task = _tasks.get(task_id)
    if not task:
        return

    task.status = 'running'
    task.message = '初始化中...'

    try:
        cfg = _validate_config(task.config)
        repo_root = _repo_root
        print(f'[BT] task={task_id} market={cfg.market} hold_hours={cfg.hold_hours} leverage={cfg.leverage} full_coverage={cfg.full_coverage} start={cfg.start_dt} end={cfg.end_dt}', flush=True)

        def on_progress(done: int, total: int, dt: str, step: str = '') -> None:
            if task.status == 'cancelled':
                raise InterruptedError('任务已取消')
            pct = int(done / max(total, 1) * 100)
            task.progress = pct
            if step:
                task.message = step
            else:
                task.message = f'回测中 {dt[:10]} ({done}/{total} bars)'

        result = run_backtest(cfg, repo_root=repo_root, on_progress=on_progress)
        task.result = result
        task.status = 'done'
        task.progress = 100
        task.message = '完成'
    except InterruptedError:
        task.status = 'cancelled'
        task.message = '已取消'
    except Exception as e:
        import traceback
        print(f'[BT ERROR] task={task_id}: {e}', flush=True)
        traceback.print_exc()
        task.status = 'failed'
        task.error = str(e)
        task.message = f'失败: {e}'
    finally:
        task.finished_at = time.time()


def submit_task(user: str, config: dict) -> str:
    """提交回测任务，返回 task_id。超过并发限制时抛出 RuntimeError"""
    task_id = str(uuid.uuid4())

    with _tasks_lock:
        _cleanup_old_tasks()
        running_count = sum(
            1 for t in _tasks.values()
            if t.user == user and t.status in ('pending', 'running')
        )
        if running_count >= MAX_TASKS_PER_USER:
            raise RuntimeError(f'任务数超限（最多 {MAX_TASKS_PER_USER} 个并发），请等待已有任务完成')
        task = BacktestTask(task_id=task_id, user=user, config=config)
        _tasks[task_id] = task

    t = threading.Thread(target=_run_task, args=(task_id,), daemon=True)
    t.start()
    return task_id


def get_task_status(task_id: str, user: str) -> dict:
    with _tasks_lock:
        t = _tasks.get(task_id)
    if not t or t.user != user:
        return {'ok': False, 'error': 'not_found'}
    return {
        'ok': True,
        'task_id': task_id,
        'status': t.status,
        'progress': t.progress,
        'message': t.message,
        'error': t.error,
    }


def get_task_result(task_id: str, user: str) -> dict | None:
    with _tasks_lock:
        t = _tasks.get(task_id)
    if not t or t.user != user:
        return None
    if t.status != 'done' or t.result is None:
        return None
    from dataclasses import asdict
    r = t.result
    return {
        'config_snapshot': r.config_snapshot,
        'stats': asdict(r.stats),
        'equity_curve': r.equity_curve,
        'symbol_hits': r.symbol_hits,
        'top_symbols': r.top_symbols,
        'generated_at': r.generated_at,
    }


def cancel_task(task_id: str, user: str) -> bool:
    with _tasks_lock:
        t = _tasks.get(task_id)
    if not t or t.user != user:
        return False
    if t.status in ('pending', 'running'):
        t.status = 'cancelled'
        t.finished_at = time.time()
    return True


def list_user_tasks(user: str) -> list[dict]:
    with _tasks_lock:
        tasks = [t for t in _tasks.values() if t.user == user]
    return [
        {
            'task_id': t.task_id,
            'status': t.status,
            'progress': t.progress,
            'message': t.message,
            'created_at': t.created_at,
            'finished_at': t.finished_at,
        }
        for t in sorted(tasks, key=lambda x: -x.created_at)
    ]
