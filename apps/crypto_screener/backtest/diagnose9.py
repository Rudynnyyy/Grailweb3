#!/usr/bin/env python3
"""直接用 apply_all_filters 诊断 selected=0"""
import os, sys
os.environ['QC_MERGE_SWAP_PATH'] = '/home/ubuntu/数据获取/data/swap_lin'
os.environ['QC_MERGE_SPOT_PATH'] = '/home/ubuntu/数据获取/data/spot_lin'
os.environ['QC_PKL_REQUIRE_FRESH'] = '0'
sys.path.insert(0, '/home/ubuntu')

from pathlib import Path
from datetime import datetime, timezone
from apps.crypto_screener.backtest.engine import _load_universe, _build_time_axis, _build_row_at
from apps.crypto_screener.app.filter_engine import apply_all_filters

repo_root = Path('/home/ubuntu')
print('加载数据...')
all_series = _load_universe('swap', repo_root, [], [])
print(f'加载完成: {len(all_series)} 个符号')

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

series_dt_index = {
    k: {_norm_dt(dt): i for i, dt in enumerate(s.dt)}
    for k, s in all_series.items()
}

test_dt = time_axis[0]
rows = []
for key, s in all_series.items():
    idx = series_dt_index[key].get(test_dt)
    if idx is None or idx < 199: continue
    row = _build_row_at(s, idx, 200)
    rows.append(row)
print(f'rows: {len(rows)}')

# 测试不同的 filter_config
configs_to_test = [
    ('market=swap condCloseMa', {
        'params': {'maPeriodClose': 20, 'market': 'swap'},
        'toggles': {'condCloseMa': True},
        'customFactors': [], 'lists': {'whitelist': [], 'blacklist': []},
        '_disable_pkl': True,
        'sort': {'key': 'pct_change', 'order': 'desc'},
    }),
    ('market=all condCloseMa', {
        'params': {'maPeriodClose': 20, 'market': 'all'},
        'toggles': {'condCloseMa': True},
        'customFactors': [], 'lists': {'whitelist': [], 'blacklist': []},
        '_disable_pkl': True,
        'sort': {'key': 'pct_change', 'order': 'desc'},
    }),
    ('no toggles', {
        'params': {'market': 'swap'},
        'toggles': {},
        'customFactors': [], 'lists': {'whitelist': [], 'blacklist': []},
        '_disable_pkl': True,
        'sort': {'key': 'pct_change', 'order': 'desc'},
    }),
]

for label, fc in configs_to_test:
    # 注意：apply_all_filters 会修改 rows，需要深拷贝
    import copy
    rows_copy = copy.deepcopy(rows)
    result = apply_all_filters(rows_copy, fc)
    print(f'[{label}] selected={len(result["selected"])} filteredOut={result["filteredOut"]}')
    # 打印第一个 row 的 market 字段
    if rows_copy:
        print(f'  row[0].market = {rows_copy[0].get("market")}  filter market = {fc["params"].get("market")}')
