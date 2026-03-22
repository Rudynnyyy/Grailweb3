#!/usr/bin/env python3
"""精确定位 condCloseMa 为何过滤所有币种"""
import os, sys, copy
os.environ['QC_MERGE_SWAP_PATH'] = '/home/ubuntu/数据获取/data/swap_lin'
os.environ['QC_PKL_REQUIRE_FRESH'] = '0'
sys.path.insert(0, '/home/ubuntu')

from pathlib import Path
from datetime import datetime, timezone
from apps.crypto_screener.backtest.engine import _load_universe, _build_time_axis, _build_row_at
from apps.crypto_screener.app.filter_engine import compute_builtins, sma, get_series

repo_root = Path('/home/ubuntu')
all_series = _load_universe('swap', repo_root, [], [])
time_axis = _build_time_axis(all_series, '2026-03-01T00:00:00+00:00', '2026-03-08T00:00:00+00:00')

def _norm_dt(s):
    try:
        s = s.strip()
        if s.endswith('Z'): s = s[:-1] + '+00:00'
        d = datetime.fromisoformat(s)
        if d.tzinfo is None: d = d.replace(tzinfo=timezone.utc)
        else: d = d.astimezone(timezone.utc)
        return d.strftime('%Y-%m-%dT%H:%M:%S+00:00')
    except: return s

series_dt_index = {k: {_norm_dt(dt): i for i, dt in enumerate(s.dt)} for k, s in all_series.items()}
test_dt = time_axis[0]

rows = []
for key, s in all_series.items():
    idx = series_dt_index[key].get(test_dt)
    if idx is None or idx < 199: continue
    rows.append(_build_row_at(s, idx, 200))

params = {'maPeriodClose': 20, 'market': 'swap', '_disable_pkl': True}

print(f'检查前10个row的 compute_builtins 结果:')
for row in rows[:10]:
    builtins = compute_builtins(row, params)
    closes = get_series(row, 'close')
    last_close = row.get('close')
    ma_key = f'ma_{int(params["maPeriodClose"])}'
    ma_val = builtins.get(ma_key)
    manual_ma = sma(closes, 20)
    print(f'  {row["symbol"]}: last_close={last_close} builtins[{ma_key}]={ma_val} manual_sma={manual_ma} closes_len={len(closes)}')
    if ma_val is None and manual_ma is not None:
        print(f'    !! builtins 返回 None 但手动算有值，检查 params 键名')
        print(f'    params keys: {list(params.keys())}')
        # 检查 compute_builtins 里的逻辑
        ps = [params.get('maPeriodClose'), params.get('maFast'), params.get('maSlow')]
        print(f'    ps = {ps}')
