"""backtest/engine.py — v2"""
from __future__ import annotations
import json, os, pickle, sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
_repo_root = Path(__file__).resolve().parents[3]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
from apps.crypto_screener.app.filter_engine import apply_all_filters, sort_rows
from apps.crypto_screener.app.series_source import SymbolSeries, load_symbol_series

@dataclass
class BacktestConfig:
    params: dict = field(default_factory=dict)
    toggles: dict = field(default_factory=dict)
    custom_factors: list = field(default_factory=list)
    market: str = 'swap'
    start_dt: str = ''
    end_dt: str = ''
    hold_hours: int = 1
    tail_len: int = 360
    top_n: int = 0
    fee_rate: float = 0.0005
    leverage: float = 1.0
    direction: str = 'long'
    sort_key: str = 'pct_change'  # 排序字段，如 pct_change / ma_20 / rsi_14
    sort_order: str = 'desc'      # 'desc'降序 / 'asc'升序
    full_coverage: bool = False   # True=每Bar独立开仓（全时段滚动），False=无重叠顺序开仓
    whitelist: list = field(default_factory=list)
    blacklist: list = field(default_factory=list)

@dataclass
class BacktestBar:
    dt: str
    n_signals: int
    n_held: int
    bar_return: float
    cum_return: float
    symbols_held: list

@dataclass
class BacktestStats:
    total_return: float
    annualized_return: float
    max_drawdown: float
    sharpe_ratio: float
    calmar_ratio: float
    win_rate: float
    avg_signals_per_bar: float
    avg_held_per_bar: float
    total_bars: int
    signal_bars: int
    blank_bars: int
    total_trades: int
    duration_hours: int

@dataclass
class BacktestResult:
    config_snapshot: dict
    stats: BacktestStats
    equity_curve: list
    symbol_hits: dict
    top_symbols: list
    generated_at: str

# --- combine path ---
def _combine_dir() -> Path | None:
    env = (os.environ.get('QC_COMBINE_DIR') or '').strip()
    p = Path(env) if env else Path(r'D:\量化交易\数据\combine')
    return p if p.exists() else None

# --- fast loader ---
def _load_universe_fast(market: str, whitelist: list, blacklist: list) -> dict[str, SymbolSeries]:
    import pandas as _pd, numpy as _np, time as _t
    combine = _combine_dir()
    if combine is None:
        return {}
    def _lpkl(name):
        fp = combine / name
        if not fp.exists(): return None
        try:
            with fp.open('rb') as f: return pickle.load(f)
        except Exception as e:
            print(f'[BT fast] load {name} error: {e}', flush=True); return None
    wl = {s.upper() for s in whitelist if s} if whitelist else set()
    bl = {s.upper() for s in blacklist if s} if blacklist else set()
    def _base(sym):
        s = sym.upper()
        if s.endswith('-USDT'): return s[:-5]
        if s.endswith('USDT'): return s[:-4]
        return s.split('-')[0] if '-' in s else s
    def _ok(sym):
        u, b = sym.upper(), _base(sym)
        if wl and u not in wl and b not in wl: return False
        if bl and (u in bl or b in bl): return False
        return True
    mkts = []
    if market in ('swap', 'all'): mkts.append('swap')
    if market in ('spot', 'all'): mkts.append('spot')
    all_series: dict[str, SymbolSeries] = {}
    for mkt in mkts:
        t0 = _t.time()
        pv_data = _lpkl(f'market_pivot_{mkt}.pkl')
        if pv_data is None:
            print(f'[BT fast] no pivot for {mkt}', flush=True); continue
        pv_close = pv_data.get('close')
        pv_open  = pv_data.get('open')
        if pv_close is None or pv_close.empty:
            print(f'[BT fast] empty pivot close for {mkt}', flush=True); continue
        pidx = pv_close.index
        if pidx.tz is None: pidx = pidx.tz_localize('UTC')
        else: pidx = pidx.tz_convert('UTC')
        dt_strs = [ts.strftime('%Y-%m-%dT%H:%M:%S+00:00') for ts in pidx]
        print(f'[BT fast] {mkt} pivot {len(dt_strs)} bars {len(pv_close.columns)} syms t={_t.time()-t0:.1f}s', flush=True)
        t1 = _t.time()
        dd = _lpkl(f'{mkt}_dict.pkl') or {}
        print(f'[BT fast] {mkt} dict {len(dd)} syms t={_t.time()-t1:.1f}s', flush=True)
        t2 = _t.time()
        aux_cols = ('high', 'low', 'volume', 'quote_volume')
        dict_aux: dict[str, dict] = {}
        for sr, df in dd.items():
            if not isinstance(df, _pd.DataFrame) or df.empty: continue
            if 'candle_begin_time' not in df.columns: continue
            need = [c for c in aux_cols if c in df.columns]
            if not need: continue
            sub = df[['candle_begin_time'] + need].dropna(subset=['candle_begin_time']).set_index('candle_begin_time')
            rec: dict = {c: sub[c].to_numpy() for c in need if c in sub.columns}
            idx = sub.index
            if idx.tz is None: idx = idx.tz_localize('UTC')
            else: idx = idx.tz_convert('UTC')
            rec['_idx'] = idx
            dict_aux[str(sr).strip()] = rec
        print(f'[BT fast] {mkt} aux extracted t={_t.time()-t2:.1f}s', flush=True)
        t3 = _t.time(); added = 0; skipped = 0
        for sym_raw in pv_close.columns:
            sym = str(sym_raw).strip()
            if not _ok(sym): skipped += 1; continue
            ca = pv_close[sym_raw]
            vm = ca.notna().values
            vi = _np.where(vm)[0]
            if len(vi) < 48: skipped += 1; continue
            fv, lv = int(vi[0]), int(vi[-1]) + 1
            dt_sl = dt_strs[fv:lv]
            cv = ca.values[fv:lv]
            close_l = [None if _np.isnan(v) else float(v) for v in cv]
            out: dict[str, list] = {'close': close_l}
            if pv_open is not None and sym_raw in pv_open.columns:
                ov = pv_open[sym_raw].values[fv:lv]
                out['open'] = [None if _np.isnan(v) else float(v) for v in ov]
            aux = dict_aux.get(sym)
            if aux:
                aidx = aux.get('_idx')
                if aidx is not None and len(aidx) > 0:
                    pv_sl = pidx[fv:lv]
                    mask = aidx.isin(pv_sl)
                    pv_pos = {ts: i for i, ts in enumerate(pv_sl)}
                    aligned = {c: [None] * len(dt_sl) for c in aux_cols if c in aux}
                    for ai, ats in enumerate(aidx):
                        if ats in pv_pos:
                            pi = pv_pos[ats]
                            for c in aligned:
                                arr = aux[c]
                                if ai < len(arr):
                                    v2 = arr[ai]
                                    aligned[c][pi] = None if (_np.isnan(float(v2)) if v2 is not None else True) else float(v2)
                    for c, vals in aligned.items():
                        out[c] = vals
            all_series[f'{mkt}:{sym}'] = SymbolSeries(market=mkt, symbol=sym, dt=dt_sl, series=out, source='combine_pkl')
            added += 1
        print(f'[BT fast] {mkt} added={added} skipped={skipped} t={_t.time()-t3:.1f}s', flush=True)
    return all_series

# --- original CSV loader (fallback) ---
def _load_universe(market, repo_root, whitelist, blacklist):
    latest_path = repo_root / 'apps' / 'crypto_screener' / 'web' / 'data' / 'latest.json'
    if not latest_path.exists(): return {}
    try: latest = json.loads(latest_path.read_text(encoding='utf-8'))
    except Exception: return {}
    results = latest.get('results', [])
    if not isinstance(results, list): return {}
    symbols = [
        (str(r.get('symbol','')), str(r.get('market','')))
        for r in results
        if isinstance(r, dict) and r.get('symbol') and r.get('market')
        and (market == 'all' or r.get('market') == market)
    ]
    if whitelist:
        wl = {s.upper() for s in whitelist if s}
        symbols = [(s,m) for s,m in symbols if s.upper() in wl or s.split('-')[0].upper() in wl]
    if blacklist:
        bl = {s.upper() for s in blacklist if s}
        symbols = [(s,m) for s,m in symbols if s.upper() not in bl and s.split('-')[0].upper() not in bl]
    print(f'[BT engine] loading {len(symbols)} symbols CSV, market={market}', flush=True)
    from apps.crypto_screener.app.series_source import _default_merge_dirs, _default_data_center_dirs, read_merge_csv_tail, _pick_existing_csv
    _sw, _sp = _default_merge_dirs(repo_root)
    _dc_sw, _dc_sp = _default_data_center_dirs(repo_root)
    print(f'[BT engine] swap dirs: merge={_sw} dc={_dc_sw}', flush=True)
    import pandas as _pd
    from datetime import timezone as _tz
    all_series: dict[str, SymbolSeries] = {}
    for symbol, mkt in symbols:
        try:
            if str(mkt).lower() == 'swap':
                search_dirs = [_sw, _dc_sw]
            else:
                search_dirs = [_sp, _dc_sp]
            csv_path, picked_sym = _pick_existing_csv(search_dirs, str(symbol).strip(), tail_hint=9999)
            if csv_path is None: continue
            df = read_merge_csv_tail(csv_path, tail=9999)
            if df.empty: continue
            out: dict[str, list] = {}
            for col in ('open','high','low','close','volume','quote_volume'):
                if col in df.columns:
                    vals = _pd.to_numeric(df[col], errors='coerce').tolist()
                    out[col] = [None if (v is None or (isinstance(v,float) and _pd.isna(v))) else float(v) for v in vals]
            dt_list = []
            for x in df['candle_begin_time'].tolist():
                try: dt_list.append(x.astimezone(_tz.utc).isoformat())
                except Exception:
                    dt_list.append(_pd.to_datetime(x, utc=True, errors='coerce').to_pydatetime().replace(tzinfo=_tz.utc).isoformat())
            if len(dt_list) < 48: continue
            all_series[f'{mkt}:{symbol}'] = SymbolSeries(market=str(mkt).lower(), symbol=picked_sym or str(symbol), dt=dt_list, series=out, source='csv')
        except Exception as _e:
            print(f'[BT engine] error {mkt}:{symbol}: {_e}', flush=True)
    print(f'[BT engine] loaded {len(all_series)} series', flush=True)
    return all_series


def _build_time_axis(all_series: dict[str, SymbolSeries], start_dt: str, end_dt: str) -> list[str]:
    from collections import Counter
    def _norm(s: str) -> str:
        try:
            s = s.strip()
            if s.endswith('Z'): s = s[:-1] + '+00:00'
            elif s.endswith('.000Z'): s = s[:-5] + '+00:00'
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
            else: dt = dt.astimezone(timezone.utc)
            return dt.strftime('%Y-%m-%dT%H:%M:%S+00:00')
        except Exception: return s
    start_norm = _norm(start_dt)
    end_norm   = _norm(end_dt)
    print(f'[BT time_axis] start={start_norm} end={end_norm}', flush=True)
    dt_counter: Counter = Counter()
    for s in all_series.values():
        for dt in s.dt:
            dt_n = _norm(dt)
            if start_norm <= dt_n <= end_norm:
                dt_counter[dt_n] += 1
    if not dt_counter:
        return []
    result = sorted(dt_counter.keys())
    print(f'[BT time_axis] total bars={len(result)}', flush=True)
    return result


def _build_row_at(series: SymbolSeries, bar_idx: int, tail: int) -> dict:
    start = max(0, bar_idx - tail + 1)
    end = bar_idx + 1
    sliced: dict[str, list] = {
        col: series.series[col][start:end]
        for col in ('open', 'high', 'low', 'close', 'volume', 'quote_volume')
        if col in series.series
    }
    closes = sliced.get('close', [])
    last_close = closes[-1] if closes else None
    return {
        'symbol': series.symbol,
        'market': series.market,
        'close': last_close,
        'series': sliced,
        '_builtins': {},
        '_expr': {},
    }


def _calc_bar_return(
    held: list[dict],
    all_series: dict[str, SymbolSeries],
    series_dt_index: dict[str, dict[str, int]],
    entry_dt: str,
    hold_hours: int,
    fee_rate: float,
    leverage: float = 1.0,
    direction: str = 'long',
) -> float:
    if not held:
        return 0.0
    rets = []
    for row in held:
        key = f"{row['market']}:{row['symbol']}"
        s = all_series.get(key)
        if s is None: continue
        idx_map = series_dt_index.get(key, {})
        entry_idx = idx_map.get(entry_dt)
        if entry_idx is None: continue
        exit_idx = entry_idx + hold_hours
        closes = s.series.get('close', [])
        if exit_idx >= len(closes): continue
        c0 = closes[entry_idx]
        c1 = closes[exit_idx]
        if c0 is None or c1 is None or c0 == 0: continue
        price_ret = (c1 / c0 - 1.0)
        if direction == 'short': price_ret = -price_ret
        rets.append(price_ret * leverage - 2.0 * fee_rate * leverage)
    return sum(rets) / len(rets) if rets else 0.0


def _calc_stats(bars: list[BacktestBar]) -> BacktestStats:
    rets = [b.bar_return for b in bars]
    n = len(rets)
    if n == 0:
        return BacktestStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    equity = 1.0; peak = 1.0; max_dd = 0.0; win = 0
    for r in rets:
        equity *= (1.0 + r)
        if equity > peak: peak = equity
        dd = (peak - equity) / peak if peak > 0 else 0.0
        if dd > max_dd: max_dd = dd
        if r > 0: win += 1
    # 只统计有实际持仓的Bar（n_held>0），避免空仓Bar拉偏胜率和Sharpe
    active_rets = [b.bar_return for b in bars if b.n_held > 0]
    n_active = len(active_rets)
    win_active = sum(1 for r in active_rets if r > 0)
    total_return = equity - 1.0
    hours_per_year = 8760
    # 年化收益：防止 n 太小导致指数爆炸，限制在 [-99.99%, +9999%] 范围内
    if n > 0 and equity > 0:
        ann_return = equity ** (hours_per_year / n) - 1.0
        ann_return = max(-0.9999, min(99.99, ann_return))
    else:
        ann_return = 0.0
    # Sharpe基于有持仓的Bar计算，更准确
    if n_active > 1:
        mean_r = sum(active_rets) / n_active
        variance = sum((r - mean_r) ** 2 for r in active_rets) / n_active
        std_r = variance ** 0.5
        sharpe = (mean_r / std_r * (hours_per_year ** 0.5)) if std_r > 0 else 0.0
    else:
        sharpe = 0.0
    sharpe = max(-999.0, min(999.0, sharpe))  # 防止极端值
    calmar = (ann_return / max_dd) if max_dd > 0 else 0.0
    calmar = max(-999.0, min(999.0, calmar))  # 防止极端值
    signal_bars = sum(1 for b in bars if b.n_signals > 0)
    win_rate = round(win_active / n_active, 4) if n_active > 0 else 0.0
    return BacktestStats(
        total_return=round(total_return, 6),
        annualized_return=round(ann_return, 6),
        max_drawdown=round(max_dd, 6),
        sharpe_ratio=round(sharpe, 4),
        calmar_ratio=round(calmar, 4),
        win_rate=win_rate,
        avg_signals_per_bar=round(sum(b.n_signals for b in bars) / n, 2),
        avg_held_per_bar=round(sum(b.n_held for b in bars) / n, 2),
        total_bars=n,
        signal_bars=signal_bars,
        blank_bars=n - signal_bars,
        total_trades=sum(b.n_held for b in bars),
        duration_hours=n,
    )


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------

def run_backtest(
    config: BacktestConfig,
    repo_root: Path,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> BacktestResult:
    """
    执行完整回测，返回 BacktestResult。
    全程复用实盘 filter_engine / series_source，结果数据与实盘完全一致。
    """
    # 1. 预加载全量历史序列
    if on_progress:
        on_progress(0, 100, '', step='\u23f3 步骤1/4：正在加载历史K线数据...')

    # 优先尝试 combine 预处理 pkl（秒级加载），失败时 fallback 到 CSV 链路
    print(f'[BT run_backtest] market={config.market} 尝试 combine 快速加载...', flush=True)
    all_series = _load_universe_fast(config.market, config.whitelist, config.blacklist)
    if all_series:
        print(f'[BT run_backtest] combine 快速加载成功，共 {len(all_series)} 个符号', flush=True)
    else:
        print('[BT run_backtest] combine 不可用，fallback 到 CSV 链路...', flush=True)
        if on_progress:
            on_progress(0, 100, '', step='\u23f3 步骤1/4：combine不可用，正在逐个读取CSV（较慢）...')
        all_series = _load_universe(config.market, repo_root, config.whitelist, config.blacklist)
    print(f'[BT run_backtest] 数据加载完成，共 {len(all_series)} 个符号', flush=True)

    if not all_series:
        raise ValueError('未找到任何符号的历史数据，请检查数据路径和时间范围')

    # 2. 建立时间轴
    if on_progress:
        on_progress(5, 100, '', step=f'\u23f3 步骤2/4：已加载 {len(all_series)} 个品种，正在建立时间轴...')
    time_axis = _build_time_axis(all_series, config.start_dt, config.end_dt)
    total_bars = len(time_axis)
    if total_bars == 0:
        raise ValueError(f'时间范围 [{config.start_dt}, {config.end_dt}] 内无数据')

    # 3. 建立符号→时间索引映射（O(1)查找）
    def _norm_dt(s: str) -> str:
        try:
            s = s.strip()
            if s.endswith('Z'): s = s[:-1] + '+00:00'
            d = datetime.fromisoformat(s)
            if d.tzinfo is None: d = d.replace(tzinfo=timezone.utc)
            else: d = d.astimezone(timezone.utc)
            return d.strftime('%Y-%m-%dT%H:%M:%S+00:00')
        except Exception: return s

    series_dt_index: dict[str, dict[str, int]] = {
        key: {_norm_dt(dt): i for i, dt in enumerate(s.dt)}
        for key, s in all_series.items()
    }

    # 4. 筛选配置（与实盘完全一致）
    merged_params = dict(config.params)
    merged_params['market'] = config.market
    filter_config: dict = {
        'params': merged_params,
        'toggles': config.toggles,
        'customFactors': config.custom_factors,
        'lists': {'whitelist': [], 'blacklist': []},
        '_disable_pkl': True,  # 回测禁用PKL缓存，避免未来函数污染
        'sort': {'key': config.sort_key, 'order': config.sort_order},
    }

    # 5. 逐bar回测
    if on_progress:
        on_progress(10, 100, '', step=f'\u23f3 步骤3/4：开始逐bar回测，共 {total_bars} 根K线...')
    bars: list[BacktestBar] = []
    equity = 1.0
    # full_coverage=False 时：持仓中不重复开仓，等平仓后再开（无重叠）
    # full_coverage=True  时：每Bar独立开仓（滚动持仓），等同主程序每小时都在运行
    # 注：hold_hours=1时两者效果相同（每Bar都开仓并立即平仓）
    next_entry_bar = 0  # 非全覆盖模式：下次允许开仓的bar序号
    pending_returns: list[tuple[int, float]] = []  # (平仓bar序号, 收益)

    for bar_num, dt in enumerate(time_axis):
        rows = []
        for key, s in all_series.items():
            idx = series_dt_index[key].get(dt)
            if idx is None or idx < config.tail_len - 1:
                continue
            row = _build_row_at(s, idx, config.tail_len)
            rows.append(row)

        result = apply_all_filters(rows, filter_config)
        selected = result['selected']

        if config.top_n > 0 and selected:
            sorted_selected, _sort_key = sort_rows(selected, filter_config)
            held = sorted_selected[:config.top_n]
        else:
            held = selected

        if config.full_coverage:
            # 全覆盖模式：每Bar都独立开仓，收益在当前Bar记录
            bar_ret = _calc_bar_return(
                held, all_series, series_dt_index,
                dt, config.hold_hours, config.fee_rate,
                leverage=config.leverage, direction=config.direction,
            )
            n_held_actual = len(held)
        else:
            # 非全覆盖模式：只在上一笔平仓后才开新仓
            if bar_num < next_entry_bar or not held:
                bar_ret = 0.0
                n_held_actual = 0
                held = []
            else:
                bar_ret = _calc_bar_return(
                    held, all_series, series_dt_index,
                    dt, config.hold_hours, config.fee_rate,
                    leverage=config.leverage, direction=config.direction,
                )
                n_held_actual = len(held)
                if held:
                    next_entry_bar = bar_num + config.hold_hours

        if bar_num == 0:
            print(f'[BT bar0] dt={dt} rows={len(rows)} selected={len(selected)} held={n_held_actual} mode={"full" if config.full_coverage else "seq"}', flush=True)

        equity *= (1.0 + bar_ret)
        bars.append(BacktestBar(
            dt=dt, n_signals=len(selected), n_held=n_held_actual,
            bar_return=bar_ret, cum_return=round(equity, 8),
            symbols_held=[r['symbol'] for r in held],
        ))

        if on_progress and bar_num % 5 == 0:
            pct = 10 + int(bar_num / max(total_bars, 1) * 85)
            on_progress(pct, 100, dt)

    # 6. 汇总统计
    if on_progress:
        on_progress(96, 100, '', step='\u23f3 步骤4/4：正在计算统计指标...')
    stats = _calc_stats(bars)

    symbol_hits: dict[str, int] = {}
    for b in bars:
        for sym in b.symbols_held:
            symbol_hits[sym] = symbol_hits.get(sym, 0) + 1

    top_symbols = sorted(
        [{'symbol': k, 'hits': v} for k, v in symbol_hits.items()],
        key=lambda x: -x['hits'],
    )[:20]

    return BacktestResult(
        config_snapshot=asdict(config),
        stats=stats,
        equity_curve=[
            {'dt': b.dt, 'equity': round(b.cum_return, 6),
             'n_held': b.n_held, 'bar_return': round(b.bar_return, 6),
             'symbols_held': b.symbols_held}
            for b in bars
        ],
        symbol_hits=symbol_hits,
        top_symbols=top_symbols,
        generated_at=datetime.now(timezone.utc).isoformat(),
    )
