"""
verify_fast_loader.py — 验证 combine 快速加载与端到端回测
用法: C:/anaconda3/envs/Gamma/python.exe verify_fast_loader.py
"""
import sys, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from apps.crypto_screener.backtest.engine import (
    _load_universe_fast, _combine_dir,
    BacktestConfig, run_backtest,
)
from datetime import datetime, timezone

print('=== 1. combine 目录检查 ===', flush=True)
print(f'combine_dir: {_combine_dir()}', flush=True)

print('\n=== 2. 快速加载测试（swap）===', flush=True)
t1 = time.time()
result = _load_universe_fast('swap', [], [])
t2 = time.time()
print(f'加载耗时: {t2-t1:.2f}s  符号数: {len(result)}', flush=True)

if not result:
    print('ERROR: 快速加载返回空！', flush=True)
    sys.exit(1)

k = list(result.keys())[0]
s = result[k]
print(f'抽查 {k}: dt_len={len(s.dt)} dt[0]={s.dt[0]} dt[-1]={s.dt[-1]}', flush=True)
print(f'  series keys={list(s.series.keys())}', flush=True)
print(f'  close[-3:]={s.series["close"][-3:]}', flush=True)
if "high" in s.series:
    print(f'  high[-3:]={s.series["high"][-3:]}', flush=True)

has_high = sum(1 for s in result.values() if 'high' in s.series)
has_vol  = sum(1 for s in result.values() if 'volume' in s.series)
print(f'含high: {has_high}/{len(result)}  含volume: {has_vol}/{len(result)}', flush=True)

errs = 0
for key, ser in list(result.items())[:30]:
    for dt_str in ser.dt[-2:]:
        if '+00:00' not in dt_str and not dt_str.endswith('Z'):
            print(f'  时区异常: {key} {dt_str}', flush=True)
            errs += 1
print(f'时间格式检查: {"OK" if errs==0 else str(errs)+" 错误"}', flush=True)

print('\n=== 3. 端到端回测（swap，7天，top5）===', flush=True)
cfg = BacktestConfig(
    market='swap',
    start_dt='2026-02-27T00:00:00+00:00',
    end_dt='2026-03-05T23:00:00+00:00',
    hold_hours=1,
    tail_len=60,
    top_n=5,
    fee_rate=0.0005,
    leverage=1.0,
    direction='long',
)
t3 = time.time()
bt = run_backtest(cfg, repo_root=Path(__file__).resolve().parents[3])
t4 = time.time()
print(f'回测耗时: {t4-t3:.2f}s', flush=True)
print(f'总bars: {bt.stats.total_bars}  信号bars: {bt.stats.signal_bars}', flush=True)
print(f'总收益: {bt.stats.total_return:.4%}', flush=True)
print(f'年化: {bt.stats.annualized_return:.4%}  最大回撤: {bt.stats.max_drawdown:.4%}', flush=True)
print(f'夏普: {bt.stats.sharpe_ratio:.4f}  平均持仓: {bt.stats.avg_held_per_bar}', flush=True)
print(f'top symbols: {bt.top_symbols[:5]}', flush=True)
if bt.equity_curve:
    print(f'equity_curve[0]: {bt.equity_curve[0]}', flush=True)
    print(f'equity_curve[-1]: {bt.equity_curve[-1]}', flush=True)

print('\n=== 验证完成 ===', flush=True)
