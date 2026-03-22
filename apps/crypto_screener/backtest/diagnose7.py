#!/usr/bin/env python3
"""最终验证：用正确区间跑完整回测"""
import os, sys, time
os.environ['QC_MERGE_SWAP_PATH'] = '/home/ubuntu/数据获取/data/swap_lin'
os.environ['QC_MERGE_SPOT_PATH'] = '/home/ubuntu/数据获取/data/spot_lin'
os.environ['QC_PKL_REQUIRE_FRESH'] = '0'

sys.path.insert(0, '/home/ubuntu')
from pathlib import Path
from apps.crypto_screener.backtest.engine import BacktestConfig, run_backtest

cfg = BacktestConfig(
    params={'maPeriodClose': 20, 'market': 'swap'},
    toggles={'condCloseMa': True},
    market='swap',
    start_dt='2026-03-01T00:00:00+00:00',
    end_dt='2026-03-08T00:00:00+00:00',
    hold_hours=1, tail_len=200, top_n=5,
    fee_rate=0.0005, leverage=1.0, direction='long',
    full_coverage=True,
)

print('开始回测 2026-03-01 ~ 2026-03-08 ...')
t0 = time.time()
try:
    r = run_backtest(cfg, repo_root=Path('/home/ubuntu'))
    elapsed = time.time() - t0
    print(f'耗时: {elapsed:.1f}s')
    print(f'equity_curve bars: {len(r.equity_curve)}')
    if r.equity_curve:
        print(f'首根: {r.equity_curve[0]}')
        print(f'尾根: {r.equity_curve[-1]}')
    print(f'stats: total_return={r.stats.total_return:.4f}  sharpe={r.stats.sharpe_ratio:.2f}  win_rate={r.stats.win_rate:.2%}  max_dd={r.stats.max_drawdown:.4f}')
    # 测试图表生成
    from apps.crypto_screener.backtest.charts import render_equity_png, render_topsymbols_png
    from dataclasses import asdict
    png1 = render_equity_png(equity_curve=r.equity_curve, stats=asdict(r.stats))
    print(f'净值曲线图: {len(png1)} bytes OK')
    if r.top_symbols:
        png2 = render_topsymbols_png(top_symbols=r.top_symbols)
        print(f'入选频次图: {len(png2)} bytes OK')
except Exception as e:
    import traceback; traceback.print_exc()
