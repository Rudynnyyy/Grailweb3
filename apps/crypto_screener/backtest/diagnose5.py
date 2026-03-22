#!/usr/bin/env python3
"""验证 swap_lin/spot_lin 能否被回测引擎读取"""
import os, sys
os.environ['QC_MERGE_SWAP_PATH'] = '/home/ubuntu/数据获取/data/swap_lin'
os.environ['QC_MERGE_SPOT_PATH'] = '/home/ubuntu/数据获取/data/spot_lin'
os.environ['QC_PKL_REQUIRE_FRESH'] = '0'

repo_root_str = '/home/ubuntu'
sys.path.insert(0, repo_root_str)
from pathlib import Path
repo_root = Path(repo_root_str)

from apps.crypto_screener.app.series_source import _default_merge_dirs, _pick_existing_csv, read_merge_csv_tail
_sw, _sp = _default_merge_dirs(repo_root)
print(f'swap merge dir: {_sw}  exists={_sw.exists()}')
print(f'spot merge dir: {_sp}  exists={_sp.exists()}')

# 找一个 CSV 验证能否读取
csvs = list(_sw.glob('*.csv'))
print(f'\nswap CSV 数量: {len(csvs)}')
if csvs:
    sym = csvs[0].stem  # 文件名去掉 .csv
    print(f'测试读取: {csvs[0].name}')
    csv_path, picked = _pick_existing_csv([_sw], sym, tail_hint=500)
    if csv_path:
        df = read_merge_csv_tail(csv_path, tail=500)
        print(f'  读取成功: {len(df)} 行  columns={list(df.columns)[:6]}')
        print(f'  时间范围: {df["candle_begin_time"].iloc[0]} -> {df["candle_begin_time"].iloc[-1]}')
    else:
        print(f'  !! _pick_existing_csv 未找到文件')

# 运行小范围回测
print('\n运行小范围回测（2026-02-20 ~ 2026-02-22）...')
import time
from apps.crypto_screener.backtest.engine import BacktestConfig, run_backtest
cfg = BacktestConfig(
    params={'maPeriodClose': 20, 'market': 'swap'},
    toggles={'condCloseMa': True},
    market='swap',
    start_dt='2026-02-20T00:00:00+00:00',
    end_dt='2026-02-22T00:00:00+00:00',
    hold_hours=1, tail_len=200, top_n=5,
    fee_rate=0.0005, leverage=1.0, direction='long',
    full_coverage=True,
)
t0 = time.time()
try:
    r = run_backtest(cfg, repo_root=repo_root)
    print(f'耗时: {time.time()-t0:.1f}s')
    print(f'equity_curve bars: {len(r.equity_curve)}')
    print(f'stats: total_return={r.stats.total_return:.4f}  sharpe={r.stats.sharpe_ratio:.2f}')
except Exception as e:
    import traceback; traceback.print_exc()
