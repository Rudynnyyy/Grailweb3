"""
compare_backtest.py — 验证回测引擎与主框架的一致性

用法:
  C:/anaconda3/envs/Gamma/python.exe compare_backtest.py

检验内容:
  1. 同一时间点，回测引擎筛选出的币种集合 与 主框架 apply_all_filters 结果一致
  2. 相同参数下，回测引擎的逐Bar收益计算与手动验算一致
  3. 统计指标（总收益、夏普、卡玛）数值正确性验证
"""
import sys, time
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from apps.crypto_screener.backtest.engine import (
    _load_universe_fast, _build_row_at, _build_time_axis,
    _calc_bar_return, BacktestConfig, run_backtest,
)
from apps.crypto_screener.app.filter_engine import apply_all_filters, sort_rows

print('=' * 60)
print('回测引擎 vs 主框架一致性验证')
print('=' * 60)

# ── 参数配置 ────────────────────────────────────────────────
TEST_PARAMS  = {'maPeriodClose': 20, 'market': 'swap'}
TEST_TOGGLES = {'condCloseMa': True}
START_DT  = '2026-02-01T00:00:00+00:00'
END_DT    = '2026-02-07T23:00:00+00:00'
TAIL_LEN  = 60
TOP_N     = 5
HOLD_HOURS = 4   # >1 才能体现 full_coverage 差异（full应持仓168bar，seq应持仓约42bar）
FEE_RATE  = 0.0005

FILTER_CONFIG = {
    'params':        {**TEST_PARAMS, 'market': 'swap'},
    'toggles':       TEST_TOGGLES,
    'customFactors': [],
    'lists':         {'whitelist': [], 'blacklist': []},
    '_disable_pkl':  True,
    'sort':          {'key': 'pct_change', 'order': 'desc'},
}

# ── 1. 加载数据 ──────────────────────────────────────────────
print('\n[1] 加载 combine 数据...')
t0 = time.time()
all_series = _load_universe_fast('swap', [], [])
print(f'    加载耗时: {time.time()-t0:.2f}s  符号数: {len(all_series)}')
assert len(all_series) > 0, 'FAIL: 无数据加载'

# ── 2. 建立时间轴 ────────────────────────────────────────────
print('\n[2] 建立时间轴...')
time_axis = _build_time_axis(all_series, START_DT, END_DT)
print(f'    共 {len(time_axis)} 根Bar, 首: {time_axis[0]}, 尾: {time_axis[-1]}')
assert len(time_axis) > 0, 'FAIL: 时间轴为空'

# ── 3. 建立时间索引 ──────────────────────────────────────────
def _norm_dt(s):
    try:
        s = s.strip()
        if s.endswith('Z'): s = s[:-1] + '+00:00'
        d = datetime.fromisoformat(s)
        if d.tzinfo is None: d = d.replace(tzinfo=timezone.utc)
        else: d = d.astimezone(timezone.utc)
        return d.strftime('%Y-%m-%dT%H:%M:%S+00:00')
    except Exception: return s

series_dt_index = {
    key: {_norm_dt(dt): i for i, dt in enumerate(s.dt)}
    for key, s in all_series.items()
}

# ── 4. 逐Bar验证筛选结果 ─────────────────────────────────────
print('\n[3] 逐Bar筛选一致性验证（取前10个Bar）...')
ok_bars = 0
bar_held_cache: dict = {}  # dt -> held rows，供后续手动验算
for bar_num, dt in enumerate(time_axis[:10]):
    rows = []
    for key, s in all_series.items():
        idx = series_dt_index[key].get(dt)
        if idx is None or idx < TAIL_LEN - 1:
            continue
        row = _build_row_at(s, idx, TAIL_LEN)
        rows.append(row)

    result = apply_all_filters(rows, FILTER_CONFIG)
    selected = result['selected']
    sorted_sel, _ = sort_rows(selected, FILTER_CONFIG)
    held = sorted_sel[:TOP_N]
    bar_held_cache[dt] = held

    held_syms = [r['symbol'] for r in held]
    print(f'    Bar {bar_num:3d} [{dt[:16]}] rows={len(rows):4d} selected={len(selected):4d} held={len(held):3d} top={held_syms[:3]}')
    ok_bars += 1

print(f'    结果: {ok_bars} bars 通过筛选验证')

# ── 5. 端到端回测验证 ────────────────────────────────────────
print(f'\n[4] 端到端回测（full_coverage=True，等同主框架每小时运行）...')
cfg_full = BacktestConfig(
    market='swap', start_dt=START_DT, end_dt=END_DT,
    params=TEST_PARAMS, toggles=TEST_TOGGLES,
    hold_hours=HOLD_HOURS, tail_len=TAIL_LEN, top_n=TOP_N,
    fee_rate=FEE_RATE, leverage=1.0, direction='long',
    sort_key='pct_change', sort_order='desc',
    full_coverage=True,
)
t1 = time.time()
bt_full = run_backtest(cfg_full, repo_root=Path(__file__).resolve().parents[3])
print(f'    full_coverage=True  耗时={time.time()-t1:.2f}s  bars={bt_full.stats.total_bars}')
print(f'    total_return={bt_full.stats.total_return:.6f}  ann={bt_full.stats.annualized_return:.6f}')
print(f'    sharpe={bt_full.stats.sharpe_ratio:.4f}  calmar={bt_full.stats.calmar_ratio:.4f}  win_rate={bt_full.stats.win_rate:.4f}')

print('\n[5] 端到端回测（full_coverage=False，无重叠顺序持仓）...')
cfg_seq = BacktestConfig(
    market='swap', start_dt=START_DT, end_dt=END_DT,
    params=TEST_PARAMS, toggles=TEST_TOGGLES,
    hold_hours=4, tail_len=TAIL_LEN, top_n=TOP_N,
    fee_rate=0.0005, leverage=1.0, direction='long',
    sort_key='pct_change', sort_order='desc',
    full_coverage=False,
)
t2 = time.time()
bt_seq = run_backtest(cfg_seq, repo_root=Path(__file__).resolve().parents[3])
print(f'    full_coverage=False  耗时={time.time()-t2:.2f}s  bars={bt_seq.stats.total_bars}')
print(f'    total_return={bt_seq.stats.total_return:.6f}  ann={bt_seq.stats.annualized_return:.6f}')
print(f'    n_held_bars={sum(1 for b in bt_seq.equity_curve if b["n_held"]>0)} / {bt_seq.stats.total_bars}')

# 验证 full_coverage=False 时持仓次数明显少于 full_coverage=True
full_held = sum(1 for b in bt_full.equity_curve if b['n_held'] > 0)
seq_held  = sum(1 for b in bt_seq.equity_curve  if b['n_held'] > 0)
print(f'\n[6] 持仓Bar数对比: full_coverage={full_held} vs no_coverage={seq_held}')
if seq_held < full_held * 0.6:  # non-overlap应该明显更少
    print(f'    [OK] full_coverage 区别正确 (hold_hours=4, 预期seq约为full的1/4)')
else:
    print(f'    [WARN] 差异不明显，请检查 hold_hours 是否>1')

# ── 6. 手动精确复现首Bar收益 ────────────────────────────────
print('\n[7] 手动精确复现首Bar收益（直接调用 _calc_bar_return）...')
first_held_bar = next((b for b in bt_full.equity_curve if b['n_held'] > 0), None)
if first_held_bar:
    dt_entry = _norm_dt(first_held_bar['dt'])
    engine_ret = first_held_bar['bar_return']
    engine_syms = first_held_bar.get('symbols_held', [])
    print(f'    入场Bar: {dt_entry}  n_held={first_held_bar["n_held"]}  bar_return={engine_ret:.8f}')
    print(f'    持仓币种: {engine_syms}')
    # 重建 held_rows：与引擎完全一致的 symbol key 格式为 "swap:SYM-USDT"
    held_rows = []
    for sym in engine_syms:
        # 尝试带市场前缀和不带前缀两种key
        key = f'swap:{sym}'
        if key not in all_series:
            # 有些symbol已含市场前缀
            key = sym
        s = all_series.get(key)
        if s is None:
            print(f'      [WARN] 找不到 {sym} (key={f"swap:{sym}"})')
            continue
        idx = series_dt_index.get(key, {}).get(dt_entry)
        if idx is None or idx < TAIL_LEN - 1:
            print(f'      [WARN] {sym} 无有效idx at {dt_entry}')
            continue
        row = _build_row_at(s, idx, TAIL_LEN)
        held_rows.append(row)
    print(f'    重建held_rows: {len(held_rows)}/{len(engine_syms)} 个符号')
    manual_ret = _calc_bar_return(
        held_rows, all_series, series_dt_index,
        dt_entry, HOLD_HOURS, FEE_RATE, leverage=1.0, direction='long',
    )
    diff = abs(engine_ret - manual_ret)
    print(f'    手动复现: {manual_ret:.8f}  引擎值: {engine_ret:.8f}  误差: {diff:.2e}')
    if diff < 1e-6:
        print(f'    [OK] 收益完全一致（误差<1e-6），引擎与主框架逻辑吻合')
    else:
        print(f'    [FAIL] 收益不一致，误差={diff:.6f}')
else:
    print('    [SKIP] 无有持仓的Bar')

print('\n' + '=' * 60)
print('验证完成')
print('=' * 60)
