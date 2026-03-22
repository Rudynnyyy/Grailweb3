#!/usr/bin/env python3
"""诊断为何只加载248个符号而不是539个"""
import os, sys
os.environ['QC_MERGE_SWAP_PATH'] = '/home/ubuntu/数据获取/data/swap_lin'
os.environ['QC_PKL_REQUIRE_FRESH'] = '0'
sys.path.insert(0, '/home/ubuntu')

from pathlib import Path
from apps.crypto_screener.app.series_source import _default_merge_dirs, _pick_existing_csv, read_merge_csv_tail
import json, pandas as pd
from datetime import timezone

repo_root = Path('/home/ubuntu')
_sw, _ = _default_merge_dirs(repo_root)

# 读 latest.json 获取所有 swap 符号
latest_path = repo_root / 'apps' / 'crypto_screener' / 'web' / 'data' / 'latest.json'
latest = json.loads(latest_path.read_text(encoding='utf-8'))
results = latest.get('results', [])
symbols = [(str(r['symbol']), str(r['market'])) for r in results if r.get('market') == 'swap']
print(f'latest.json swap 符号数: {len(symbols)}')

# 逐一检查跳过原因
not_found = []
too_short = []
read_err = []
ok = []

for symbol, mkt in symbols:
    try:
        csv_path, picked_sym = _pick_existing_csv([_sw], str(symbol).strip(), tail_hint=9999)
        if csv_path is None:
            not_found.append(symbol)
            continue
        df = read_merge_csv_tail(csv_path, tail=9999)
        if df.empty:
            too_short.append((symbol, 0))
            continue
        if len(df) < 48:
            too_short.append((symbol, len(df)))
            continue
        ok.append(symbol)
    except Exception as e:
        read_err.append((symbol, str(e)))

print(f'\nOK: {len(ok)}')
print(f'找不到CSV: {len(not_found)}')
if not_found[:10]:
    print(f'  示例: {not_found[:10]}')
print(f'行数不足48: {len(too_short)}')
if too_short[:10]:
    print(f'  示例: {too_short[:10]}')
print(f'读取出错: {len(read_err)}')
if read_err[:5]:
    print(f'  示例: {read_err[:5]}')

# 检查 _pick_existing_csv 的候选文件名逻辑
print('\n检查文件名匹配逻辑...')
# 找一个 not_found 的符号看 CSV 文件名是否存在
if not_found:
    sym = not_found[0]
    print(f'  not_found 示例: {sym}')
    # 找近似文件名
    upper = sym.upper().replace('-', '')
    candidates = list(_sw.glob(f'*{upper.split("USDT")[0]}*'))
    print(f'  模糊匹配结果: {[c.name for c in candidates[:5]]}')

# 检查 tail_len=200 对行数的影响
print(f'\n检查 tail_len=200 的影响...')
too_short_200 = [s for s in ok]
from apps.crypto_screener.backtest.engine import _load_universe, _build_time_axis
all_series = _load_universe('swap', repo_root, [], [])
print(f'_load_universe 实际加载: {len(all_series)} 个符号')

# 看看有多少符号在 2026-03-01 时 idx < 200-1
from datetime import datetime
def _norm_dt(s):
    try:
        s = s.strip()
        if s.endswith('Z'): s = s[:-1] + '+00:00'
        d = datetime.fromisoformat(s)
        if d.tzinfo is None: d = d.replace(tzinfo=timezone.utc)
        else: d = d.astimezone(timezone.utc)
        return d.strftime('%Y-%m-%dT%H:%M:%S+00:00')
    except: return s

time_axis = _build_time_axis(all_series, '2026-03-01T00:00:00+00:00', '2026-03-08T00:00:00+00:00')
test_dt = time_axis[0] if time_axis else None
if test_dt:
    idx_none = 0; idx_small = 0; idx_ok = 0
    for key, s in all_series.items():
        dt_index = {_norm_dt(dt): i for i, dt in enumerate(s.dt)}
        idx = dt_index.get(test_dt)
        if idx is None: idx_none += 1
        elif idx < 199: idx_small += 1
        else: idx_ok += 1
    print(f'  test_dt={test_dt}')
    print(f'  idx=None(时间不匹配): {idx_none}')
    print(f'  idx<199(历史不足200根): {idx_small}')
    print(f'  idx>=199(有效): {idx_ok}')
