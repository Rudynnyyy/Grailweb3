#!/usr/bin/env python3
"""
回测诊断脚本 — 在服务器上运行：
    python apps/crypto_screener/backtest/diagnose.py
"""
from __future__ import annotations
import sys, os, pickle
from pathlib import Path

repo_root = Path(__file__).resolve().parents[3]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

print('=' * 60)
print('【1】检查 combine PKL 路径')
env = (os.environ.get('QC_COMBINE_DIR') or '').strip()
print(f'  QC_COMBINE_DIR 环境变量: "{env}"')

# 按优先级尝试各路径
candidates = []
if env:
    candidates.append(Path(env))
candidates += [
    Path('/home/ubuntu/量化交易/数据/combine'),
    Path('/home/ubuntu/data/combine'),
    Path('/data/combine'),
    Path('/root/量化交易/数据/combine'),
    Path(r'D:\量化交易\数据\combine'),
]

combine = None
for c in candidates:
    try:
        exists = c.exists()
        print(f'  尝试: {c}  exists={exists}')
        if exists:
            combine = c
            break
    except PermissionError:
        print(f'  尝试: {c}  PermissionError（无权限）')
    except Exception as ex:
        print(f'  尝试: {c}  error={ex}')

if combine is None:
    print('  !! 所有路径均不可用，请手动设置 QC_COMBINE_DIR 环境变量')
    print('  提示：export QC_COMBINE_DIR=/你的数据路径/combine')
    sys.exit(1)
else:
    print(f'  使用路径: {combine}')

for mkt in ('swap', 'spot'):
    fp = combine / f'market_pivot_{mkt}.pkl'
    print(f'  pivot {mkt}: exists={fp.exists()}', end='')
    if fp.exists():
        with fp.open('rb') as f:
            d = pickle.load(f)
        cl = d.get('close')
        if cl is not None:
            print(f'  shape={cl.shape}  index=[{cl.index[0]} -> {cl.index[-1]}]')
        else:
            print('  (no close column)')
    else:
        print()

print()
print('【2】快速加载测试（_load_universe_fast）')
from apps.crypto_screener.backtest.engine import _load_universe_fast, _combine_dir
combine_detected = _combine_dir()
print(f'  engine._combine_dir() = {combine_detected}')
import time
t0 = time.time()
all_series = _load_universe_fast('swap', [], [])
print(f'  加载完成: {len(all_series)} 个符号  耗时={time.time()-t0:.1f}s')

if all_series:
    # 抽取第一个符号检查K线数量
    key, s = next(iter(all_series.items()))
    print(f'  示例符号: {key}  K线数={len(s.dt)}  时间=[{s.dt[0][:10]} -> {s.dt[-1][:10]}]')
else:
    print('  !! 没有加载到任何符号，请检查 combine 路径和 PKL 文件')
    sys.exit(1)

print()
print('【3】时间轴测试（2026-02-01 ~ 2026-02-03）')
from apps.crypto_screener.backtest.engine import _build_time_axis
time_axis = _build_time_axis(all_series, '2026-02-01T00:00:00+00:00', '2026-02-03T00:00:00+00:00')
print(f'  time_axis bars = {len(time_axis)}')
if time_axis:
    print(f'  首尾: {time_axis[0]} -> {time_axis[-1]}')
else:
    print('  !! time_axis 为空，说明 PKL 数据的时间戳格式与回测的 norm 逻辑不匹配')

print()
print('【4】单bar筛选测试（取 time_axis 第100根）')
from apps.crypto_screener.backtest.engine import _build_row_at
from apps.crypto_screener.app.filter_engine import apply_all_filters
from datetime import datetime, timezone

if len(time_axis) < 100:
    test_dt = time_axis[-1] if time_axis else None
else:
    test_dt = time_axis[100]

if not test_dt:
    print('  !! 没有可用的 bar，跳过')
else:
    print(f'  测试 bar: {test_dt}')
    def _norm_dt(s: str) -> str:
        try:
            s = s.strip()
            if s.endswith('Z'): s = s[:-1] + '+00:00'
            d = datetime.fromisoformat(s)
            if d.tzinfo is None: d = d.replace(tzinfo=timezone.utc)
            else: d = d.astimezone(timezone.utc)
            return d.strftime('%Y-%m-%dT%H:%M:%S+00:00')
        except Exception: return s

    series_dt_index = {
        k: {_norm_dt(dt): i for i, dt in enumerate(s.dt)}
        for k, s in all_series.items()
    }
    tail_len = 200
    rows = []
    skipped_none = 0
    skipped_short = 0
    for key, s in all_series.items():
        idx = series_dt_index[key].get(test_dt)
        if idx is None:
            skipped_none += 1
            continue
        if idx < tail_len - 1:
            skipped_short += 1
            continue
        row = _build_row_at(s, idx, tail_len)
        rows.append(row)
    print(f'  rows 构建: {len(rows)} 个  (idx=None跳过={skipped_none}, idx太小跳过={skipped_short})')

    if rows:
        filter_config = {
            'params': {'maPeriodClose': 20, 'market': 'swap'},
            'toggles': {'condCloseMa': True},
            'customFactors': [],
            'lists': {'whitelist': [], 'blacklist': []},
            '_disable_pkl': True,
            'sort': {'key': 'pct_change', 'order': 'desc'},
        }
        result = apply_all_filters(rows, filter_config)
        print(f'  筛选结果: selected={len(result["selected"])}  filteredOut={result["filteredOut"]}')
    else:
        print('  !! rows 为空，无法筛选')

print()
print('【5】完整小范围回测（2026-02-01 ~ 2026-02-03，MA20）')
os.environ['QC_PKL_REQUIRE_FRESH'] = '0'
from apps.crypto_screener.backtest.engine import BacktestConfig, run_backtest
cfg = BacktestConfig(
    params={'maPeriodClose': 20, 'market': 'swap'},
    toggles={'condCloseMa': True},
    market='swap',
    start_dt='2026-02-01T00:00:00+00:00',
    end_dt='2026-02-03T00:00:00+00:00',
    hold_hours=1,
    tail_len=200,
    top_n=5,
    fee_rate=0.0005,
    leverage=1.0,
    direction='long',
    full_coverage=True,
)
try:
    t0 = time.time()
    r = run_backtest(cfg, repo_root=repo_root)
    elapsed = time.time() - t0
    print(f'  耗时: {elapsed:.1f}s')
    print(f'  equity_curve bars: {len(r.equity_curve)}')
    print(f'  stats: total_return={r.stats.total_return:.4f}  sharpe={r.stats.sharpe_ratio:.2f}  win_rate={r.stats.win_rate:.2%}')
    if r.equity_curve:
        print(f'  首根: {r.equity_curve[0]}')
        print(f'  尾根: {r.equity_curve[-1]}')
    else:
        print('  !! equity_curve 为空！')
except Exception as e:
    import traceback
    print(f'  !! 回测失败: {e}')
    traceback.print_exc()

print()
print('【6】图表生成测试')
from apps.crypto_screener.backtest.charts import render_equity_png, render_topsymbols_png
try:
    if 'r' in dir() and r.equity_curve:
        png = render_equity_png(equity_curve=r.equity_curve, stats=r.stats.__dict__ if hasattr(r.stats,'__dict__') else vars(r.stats))
        print(f'  render_equity_png OK, size={len(png)} bytes')
    else:
        print('  (跳过，equity_curve 为空)')
except Exception as e:
    import traceback
    print(f'  !! 图表生成失败: {e}')
    traceback.print_exc()

print()
print('=' * 60)
print('诊断完成')
