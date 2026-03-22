#!/usr/bin/env python3
"""诊断 selected=0 的原因"""
import os, sys
os.environ['QC_MERGE_SWAP_PATH'] = '/home/ubuntu/数据获取/data/swap_lin'
os.environ['QC_MERGE_SPOT_PATH'] = '/home/ubuntu/数据获取/data/spot_lin'
os.environ['QC_PKL_REQUIRE_FRESH'] = '0'
sys.path.insert(0, '/home/ubuntu')

from pathlib import Path
from datetime import datetime, timezone
from apps.crypto_screener.backtest.engine import (
    _load_universe, _build_time_axis, _build_row_at
)
from apps.crypto_screener.app.filter_engine import sma, compute_builtins

repo_root = Path('/home/ubuntu')
print('加载数据...')
all_series = _load_universe('swap', repo_root, [], [])
print(f'加载完成: {len(all_series)} 个符号')

# 建时间轴
time_axis = _build_time_axis(all_series, '2026-03-01T00:00:00+00:00', '2026-03-08T00:00:00+00:00')
print(f'time_axis: {len(time_axis)} bars')

def _norm_dt(s):
    try:
        s = s.strip()
        if s.endswith('Z'): s = s[:-1] + '+00:00'
        d = datetime.fromisoformat(s)
        if d.tzinfo is None: d = d.replace(tzinfo=timezone.utc)
        else: d = d.astimezone(timezone.utc)
        return d.strftime('%Y-%m-%dT%H:%M:%S+00:00')
    except: return s

series_dt_index = {
    k: {_norm_dt(dt): i for i, dt in enumerate(s.dt)}
    for k, s in all_series.items()
}

# 取第一个 bar 诊断
test_dt = time_axis[0]
print(f'\n诊断 bar: {test_dt}')
tail_len = 200
rows = []
for key, s in all_series.items():
    idx = series_dt_index[key].get(test_dt)
    if idx is None or idx < tail_len - 1:
        continue
    row = _build_row_at(s, idx, tail_len)
    rows.append(row)
print(f'rows: {len(rows)}')

if not rows:
    print('!! rows 为空，检查 tail_len 是否太大')
    # 看看各符号的 idx 值
    idx_vals = []
    for key, s in list(all_series.items())[:20]:
        idx = series_dt_index[key].get(test_dt)
        idx_vals.append((key, idx, len(s.dt)))
    for k, idx, total in idx_vals:
        print(f'  {k}: idx={idx} total={total} need>={tail_len-1}')
else:
    # 抽查前5个 row 的 close 序列
    params = {'maPeriodClose': 20, 'market': 'swap', '_disable_pkl': True}
    passed = 0
    failed_none = 0
    failed_cond = 0
    for row in rows[:100]:
        closes = (row.get('series') or {}).get('close', [])
        none_count = sum(1 for v in closes if v is None)
        ma20 = sma(closes, 20)
        last_close = closes[-1] if closes else None
        if ma20 is None:
            failed_none += 1
        elif last_close is None or last_close <= ma20:
            failed_cond += 1
        else:
            passed += 1
    print(f'\n前100个row: passed={passed} failed_ma_none={failed_none} failed_cond={failed_cond}')
    # 打印第一个 row 详情
    r = rows[0]
    closes = (r.get('series') or {}).get('close', [])
    print(f'\n示例 row: {r["symbol"]}')
    print(f'  closes 长度: {len(closes)}')
    print(f'  None 数量: {sum(1 for v in closes if v is None)}')
    print(f'  前5: {closes[:5]}')
    print(f'  后5: {closes[-5:]}')
    print(f'  MA(20): {sma(closes, 20)}')
    print(f'  last_close: {closes[-1] if closes else None}')
