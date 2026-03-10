from __future__ import annotations

import math
import re
from dataclasses import dataclass


def _is_num(x) -> bool:
    try:
        return x is not None and x == x and math.isfinite(float(x))
    except Exception:
        return False


def last_non_null(arr: list) -> float | None:
    for i in range(len(arr) - 1, -1, -1):
        v = arr[i]
        if _is_num(v):
            return float(v)
    return None


_factor_cache_lock = None
_factor_cache: dict = {"root": "", "files": {}}
_factor_metrics_lock = None
_factor_metrics: dict = {"lookups": 0, "hits": 0, "misses": 0, "used_keys": {}, "miss_symbols": {}}
_expr_ast_cache_lock = None
_expr_ast_cache: dict[str, dict] = {}


def _repo_root() -> "Path":
    from pathlib import Path

    return Path(__file__).resolve().parents[3]


def _symbol_candidates(symbol: str) -> list[str]:
    s = str(symbol or "").strip()
    if not s:
        return []
    out: list[str] = []
    for x in (s, s.upper()):
        if x and x not in out:
            out.append(x)
    up = s.upper()
    if up.endswith("-USDT"):
        no_dash = up.replace("-", "")
        if no_dash not in out:
            out.append(no_dash)
    elif up.endswith("USDT"):
        with_dash = f"{up[:-4]}-USDT"
        if with_dash not in out:
            out.append(with_dash)
    elif "-" not in up:
        with_dash = f"{up}-USDT"
        if with_dash not in out:
            out.append(with_dash)
        no_dash = f"{up}USDT"
        if no_dash not in out:
            out.append(no_dash)
    return out


def _factor_lock():
    global _factor_cache_lock
    if _factor_cache_lock is None:
        import threading

        _factor_cache_lock = threading.Lock()
    return _factor_cache_lock


def _metrics_lock():
    global _factor_metrics_lock
    if _factor_metrics_lock is None:
        import threading

        _factor_metrics_lock = threading.Lock()
    return _factor_metrics_lock


def _expr_lock():
    global _expr_ast_cache_lock
    if _expr_ast_cache_lock is None:
        import threading

        _expr_ast_cache_lock = threading.Lock()
    return _expr_ast_cache_lock


def _compile_expr(expr: str) -> dict:
    s = str(expr or "")
    lock = _expr_lock()
    with lock:
        hit = _expr_ast_cache.get(s)
    if hit is not None:
        return hit
    ast = parse_expr_tokens(tokenize_expr(s))
    with lock:
        _expr_ast_cache[s] = ast
    return ast


def get_factor_cache_metrics() -> dict:
    lock = _metrics_lock()
    with lock:
        import json

        return json.loads(json.dumps(_factor_metrics))


def reset_factor_cache_metrics() -> None:
    lock = _metrics_lock()
    with lock:
        _factor_metrics["lookups"] = 0
        _factor_metrics["hits"] = 0
        _factor_metrics["misses"] = 0
        _factor_metrics["used_keys"] = {}
        _factor_metrics["miss_symbols"] = {}


def _last_series_value(arr: list, *, shift_n: int = 0) -> float | None:
    if not isinstance(arr, list) or not arr:
        return None
    idx = len(arr) - 1 - int(shift_n or 0)
    if idx < 0 or idx >= len(arr):
        return None
    v = arr[idx]
    return float(v) if _is_num(v) else None


def _eval_expr_scalar(node: dict, ctx: dict) -> tuple[str, object | None]:
    k = node.get("k")
    if k == "num":
        v = node.get("v")
        return ("scalar", float(v) if _is_num(v) else None)
    if k == "id":
        name = str(node.get("name") or "").lower()
        if name in ("open", "high", "low", "close", "volume", "quote_volume"):
            arr = (ctx.get("series") or {}).get(name) or []
            return ("series", arr if isinstance(arr, list) else [])
        if name == "quotevolume":
            arr = (ctx.get("series") or {}).get("quote_volume") or []
            return ("series", arr if isinstance(arr, list) else [])
        if name == "eps":
            return ("scalar", 1e-12)
        if name in ("latest_close", "latestclose"):
            v = ((ctx.get("latest") or {}).get("close")) if isinstance(ctx.get("latest"), dict) else None
            return ("scalar", float(v) if _is_num(v) else None)
        return ("scalar", None)
    if k == "member":
        return ("scalar", None)
    if k == "unary":
        op = str(node.get("op") or "")
        t, v = _eval_expr_scalar(node.get("x") or {}, ctx)
        if t == "series":
            x = _last_series_value(v if isinstance(v, list) else [])
            if x is None:
                return ("scalar", None)
            return ("scalar", -float(x) if op == "-" else float(x))
        if v is None or not _is_num(v):
            return ("scalar", None)
        return ("scalar", -float(v) if op == "-" else float(v))
    if k in ("bin", "cmp"):
        op = str(node.get("op") or "")
        ta, a = _eval_expr_scalar(node.get("a") or {}, ctx)
        tb, b = _eval_expr_scalar(node.get("b") or {}, ctx)

        def to_scalar(t0: str, v0) -> float | None:
            if t0 == "series":
                return _last_series_value(v0 if isinstance(v0, list) else [])
            if v0 is None or not _is_num(v0):
                return None
            return float(v0)

        av = to_scalar(ta, a)
        bv = to_scalar(tb, b)
        if av is None or bv is None:
            return ("scalar", None)
        if k == "bin":
            if op == "+":
                return ("scalar", av + bv)
            if op == "-":
                return ("scalar", av - bv)
            if op == "*":
                return ("scalar", av * bv)
            if op == "/":
                return ("scalar", None if bv == 0 else av / bv)
            return ("scalar", None)
        if op == ">":
            return ("scalar", 1.0 if av > bv else 0.0)
        if op == ">=":
            return ("scalar", 1.0 if av >= bv else 0.0)
        if op == "<":
            return ("scalar", 1.0 if av < bv else 0.0)
        if op == "<=":
            return ("scalar", 1.0 if av <= bv else 0.0)
        if op == "==":
            return ("scalar", 1.0 if av == bv else 0.0)
        if op == "!=":
            return ("scalar", 1.0 if av != bv else 0.0)
        return ("scalar", None)
    if k == "call":
        name = str(node.get("name") or "").lower()
        args = node.get("args") or []

        def ensure_scalar(n: dict) -> float | None:
            t0, v0 = _eval_expr_scalar(n, ctx)
            if t0 == "series":
                return _last_series_value(v0 if isinstance(v0, list) else [])
            return float(v0) if _is_num(v0) else None

        def ensure_series(n: dict) -> list:
            t0, v0 = _eval_expr_scalar(n, ctx)
            if t0 == "series" and isinstance(v0, list):
                return v0
            return []

        if name == "abs":
            if not args:
                return ("scalar", None)
            x = ensure_scalar(args[0])
            return ("scalar", None if x is None else abs(float(x)))
        if name == "shift":
            if not args:
                return ("scalar", None)
            if len(args) >= 2:
                series = ensure_series(args[0])
                n = ensure_scalar(args[1])
            else:
                series = (ctx.get("series") or {}).get("close") or []
                n = ensure_scalar(args[0])
            if not series or n is None:
                return ("scalar", None)
            return ("scalar", _last_series_value(series, shift_n=int(n)))
        if name in ("ma", "sma", "ema", "mean", "std"):
            if not args:
                return ("scalar", None)
            if len(args) >= 2:
                series = ensure_series(args[0])
                w = ensure_scalar(args[1])
            else:
                series = (ctx.get("series") or {}).get("close") or []
                w = ensure_scalar(args[0])
            if not series or w is None:
                return ("scalar", None)
            win = int(w)
            if name == "std":
                return ("scalar", rolling_std(series, win))
            if name == "ema":
                return ("scalar", ema(series, win))
            return ("scalar", sma(series, win))
        if name == "rsi":
            if not args:
                return ("scalar", None)
            if len(args) >= 2:
                series = ensure_series(args[0])
                p = ensure_scalar(args[1])
            else:
                series = (ctx.get("series") or {}).get("close") or []
                p = ensure_scalar(args[0])
            if not series or p is None:
                return ("scalar", None)
            return ("scalar", rsi(series, int(p)))
        if name == "corr":
            if len(args) < 3:
                return ("scalar", None)
            x = ensure_series(args[0])
            y = ensure_series(args[1])
            w = ensure_scalar(args[2])
            if not x or not y or w is None:
                return ("scalar", None)
            return ("scalar", rolling_corr(x, y, int(w)))
        return ("scalar", None)
    return ("scalar", None)


def _factor_cache_root() -> "Path":
    from pathlib import Path
    import os

    env = (os.environ.get("QC_PKL_CACHE_ROOT") or "").strip()
    if env:
        return Path(env)
    out_root = (os.environ.get("QC_PREPROCESS_OUT_ROOT") or "").strip()
    if out_root:
        return Path(out_root) / "pkl_cache"
    repo_root = _repo_root()
    return (repo_root / "数据获取" / "data" / "preprocessed_hourly") / "pkl_cache"


def _load_factor_payload(*, market: str) -> dict:
    import os
    from pathlib import Path

    root = _factor_cache_root()
    fp = Path(root) / f"factors_{str(market).lower()}.pkl"
    if not fp.exists():
        return {}
    try:
        st_mtime_ns = int(fp.stat().st_mtime_ns)
    except Exception:
        st_mtime_ns = -1
    lock = _factor_lock()
    with lock:
        if _factor_cache.get("root") == str(root):
            files0 = _factor_cache.get("files")
            if isinstance(files0, dict):
                rec = files0.get(str(market).lower())
                if isinstance(rec, dict) and int(rec.get("mtime_ns", -1)) == st_mtime_ns:
                    data0 = rec.get("data")
                    return data0 if isinstance(data0, dict) else {}
        try:
            import pickle

            with fp.open("rb") as f:
                payload = pickle.load(f)
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        if _factor_cache.get("root") != str(root):
            _factor_cache["root"] = str(root)
            _factor_cache["files"] = {}
        files1 = _factor_cache.get("files")
        if not isinstance(files1, dict):
            files1 = {}
            _factor_cache["files"] = files1
        files1[str(market).lower()] = {"mtime_ns": st_mtime_ns, "data": payload}
        return payload


def _get_cached_factors(*, market: str, symbol: str) -> dict | None:
    if not market or not symbol:
        return None
    lock = _metrics_lock()
    with lock:
        _factor_metrics["lookups"] = int(_factor_metrics.get("lookups") or 0) + 1
    try:
        import os
        from pathlib import Path

        if str(os.environ.get("QC_PKL_REQUIRE_FRESH") or "1").strip() != "0":
            root = _factor_cache_root()
            fp = Path(root) / f"factors_{str(market).lower()}.pkl"
            try:
                fac_mtime_ns = int(fp.stat().st_mtime_ns)
            except Exception:
                fac_mtime_ns = -1
            if fac_mtime_ns > 0:
                try:
                    from apps.crypto_screener.app import series_source as ss  # noqa: E402

                    repo_root = _repo_root()
                    swap_dir, spot_dir = ss._default_merge_dirs(repo_root)
                    dc_swap_dir, dc_spot_dir = ss._default_data_center_dirs(repo_root)
                    is_swap = str(market).lower() == "swap"
                    base = swap_dir if is_swap else spot_dir
                    dc_base = dc_swap_dir if is_swap else dc_spot_dir
                    csv_path, _picked = ss._pick_existing_csv([base, dc_base], str(symbol or "").strip(), tail_hint=720)
                    if csv_path is not None:
                        try:
                            csv_mtime_ns = int(csv_path.stat().st_mtime_ns)
                        except Exception:
                            csv_mtime_ns = -1
                        if csv_mtime_ns > 0 and csv_mtime_ns > fac_mtime_ns:
                            lock = _metrics_lock()
                            with lock:
                                _factor_metrics["misses"] = int(_factor_metrics.get("misses") or 0) + 1
                                ms = _factor_metrics.get("miss_symbols")
                                if not isinstance(ms, dict):
                                    ms = {}
                                    _factor_metrics["miss_symbols"] = ms
                                k = str(symbol).upper()
                                ms[k] = int(ms.get(k) or 0) + 1
                            return None
                except Exception:
                    pass
    except Exception:
        pass
    payload = _load_factor_payload(market=str(market).lower())
    mp = payload.get("factors") if isinstance(payload, dict) else None
    if not isinstance(mp, dict):
        lock = _metrics_lock()
        with lock:
            _factor_metrics["misses"] = int(_factor_metrics.get("misses") or 0) + 1
        return None
    for s in _symbol_candidates(symbol):
        rec = mp.get(str(s).upper())
        if isinstance(rec, dict):
            lock = _metrics_lock()
            with lock:
                _factor_metrics["hits"] = int(_factor_metrics.get("hits") or 0) + 1
            return rec
    lock = _metrics_lock()
    with lock:
        _factor_metrics["misses"] = int(_factor_metrics.get("misses") or 0) + 1
        ms = _factor_metrics.get("miss_symbols")
        if not isinstance(ms, dict):
            ms = {}
            _factor_metrics["miss_symbols"] = ms
        k = str(symbol).upper()
        ms[k] = int(ms.get(k) or 0) + 1
    return None


def _mark_factor_key_used(key: str) -> None:
    if not key:
        return
    lock = _metrics_lock()
    with lock:
        mp = _factor_metrics.get("used_keys")
        if not isinstance(mp, dict):
            mp = {}
            _factor_metrics["used_keys"] = mp
        mp[str(key)] = int(mp.get(str(key)) or 0) + 1


def get_series(row: dict, name: str) -> list:
    s = (row or {}).get("series") or {}
    v = s.get(name)
    return v if isinstance(v, list) else []


def sma(arr: list, window: int) -> float | None:
    w = int(window)
    if w <= 0:
        return None
    if not isinstance(arr, list) or len(arr) < w:
        return None
    s = 0.0
    for i in range(len(arr) - w, len(arr)):
        v = arr[i]
        if not _is_num(v):
            return None
        s += float(v)
    return s / float(w)


def ema(arr: list, window: int) -> float | None:
    w = int(window)
    if w <= 0:
        return None
    xs = [float(v) for v in (arr or []) if _is_num(v)]
    if len(xs) < w:
        return None
    alpha = 2.0 / (w + 1.0)
    e = xs[0]
    for v in xs[1:]:
        e = alpha * v + (1.0 - alpha) * e
    return e


def ema_series(arr: list, window: int) -> list:
    w = int(window)
    if w <= 0:
        return []
    out = [None] * len(arr)
    alpha = 2.0 / (w + 1.0)
    first_idx = -1
    for i, v in enumerate(arr):
        if _is_num(v):
            first_idx = i
            break
    if first_idx == -1:
        return out
    last_e = float(arr[first_idx])
    out[first_idx] = last_e
    for i in range(first_idx + 1, len(arr)):
        v = arr[i]
        if _is_num(v):
            last_e = alpha * float(v) + (1.0 - alpha) * last_e
            out[i] = last_e
        else:
            out[i] = last_e
    return out


def rolling_std(series: list, window: int) -> float | None:
    w = int(window)
    if w <= 0:
        return None
    if not isinstance(series, list) or len(series) < w:
        return None
    tail = series[-w:]
    if any(not _is_num(x) for x in tail):
        return None
    xs = [float(x) for x in tail]
    mean = sum(xs) / float(w)
    var0 = sum((x - mean) * (x - mean) for x in xs) / float(w)
    return math.sqrt(var0)


def rolling_corr(x: list, y: list, window: int) -> float | None:
    w = int(window)
    if w <= 1:
        return None
    if not isinstance(x, list) or not isinstance(y, list):
        return None
    n = min(len(x), len(y))
    if n < w:
        return None
    xs = x[-w:]
    ys = y[-w:]
    for i in range(w):
        if not _is_num(xs[i]) or not _is_num(ys[i]):
            return None
    ax = [float(v) for v in xs]
    ay = [float(v) for v in ys]
    mx = sum(ax) / float(w)
    my = sum(ay) / float(w)
    cov = 0.0
    vx = 0.0
    vy = 0.0
    for i in range(w):
        dx = ax[i] - mx
        dy = ay[i] - my
        cov += dx * dy
        vx += dx * dx
        vy += dy * dy
    if vx == 0.0 or vy == 0.0:
        return None
    return cov / math.sqrt(vx * vy)


def rsi(arr: list, period: int) -> float | None:
    p = int(period)
    if p <= 0:
        return None
    xs = [float(v) for v in (arr or []) if _is_num(v)]
    if len(xs) < p + 1:
        return None
    deltas = [xs[i] - xs[i - 1] for i in range(1, len(xs))]
    if len(deltas) < p:
        return None
    avg_gain = 0.0
    avg_loss = 0.0
    for i in range(p):
        d = deltas[i]
        avg_gain += d if d > 0 else 0.0
        avg_loss += (-d) if d < 0 else 0.0
    avg_gain /= float(p)
    avg_loss /= float(p)
    for i in range(p, len(deltas)):
        d = deltas[i]
        g = d if d > 0 else 0.0
        l = (-d) if d < 0 else 0.0
        avg_gain = (avg_gain * (p - 1) + g) / float(p)
        avg_loss = (avg_loss * (p - 1) + l) / float(p)
    if avg_loss == 0.0 and avg_gain == 0.0:
        return 0.0
    if avg_loss == 0.0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def supertrend(highs: list, lows: list, closes: list, period: int, mult: float) -> float | None:
    p = int(period)
    m = float(mult)
    if len(closes) < p + 1:
        return None
    trs: list[float] = []
    for i in range(1, len(closes)):
        h = float(highs[i])
        l = float(lows[i])
        pc = float(closes[i - 1])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    alpha = 1.0 / float(p)
    atrs: list[float | None] = [None] * len(closes)
    current_atr = trs[0]
    for i in range(1, len(trs)):
        current_atr = alpha * trs[i] + (1.0 - alpha) * current_atr
        atrs[i + 1] = current_atr

    trend = 1
    upper_band = 0.0
    lower_band = 0.0
    super_t = 0.0
    for i in range(p, len(closes)):
        mid = (float(highs[i]) + float(lows[i])) / 2.0
        a = atrs[i]
        if a is None:
            continue
        basic_upper = mid + m * float(a)
        basic_lower = mid - m * float(a)
        prev_close = float(closes[i - 1])
        upper_band = basic_upper if (basic_upper < upper_band or prev_close > upper_band) else upper_band
        lower_band = basic_lower if (basic_lower > lower_band or prev_close < lower_band) else lower_band
        if trend == 1:
            if float(closes[i]) < lower_band:
                trend = -1
                super_t = upper_band
            else:
                super_t = lower_band
        else:
            if float(closes[i]) > upper_band:
                trend = 1
                super_t = lower_band
            else:
                super_t = upper_band
    return float(super_t)


def kdj(highs: list, lows: list, closes: list, n: int, m1: int, m2: int) -> dict:
    period = int(n)
    if len(closes) < period:
        return {"k": None, "d": None, "j": None}
    rsvs: list[float] = []
    for i in range(period - 1, len(closes)):
        c = float(closes[i])
        hs = [float(v) for v in highs[i - period + 1 : i + 1]]
        ls = [float(v) for v in lows[i - period + 1 : i + 1]]
        h = max(hs)
        l = min(ls)
        rsvs.append(50.0 if h == l else ((c - l) / (h - l)) * 100.0)
    k_series: list[float] = []
    last_k = 50.0
    alpha1 = 1.0 / float(int(m1))
    for r in rsvs:
        last_k = alpha1 * float(r) + (1.0 - alpha1) * last_k
        k_series.append(last_k)
    alpha2 = 1.0 / float(int(m2))
    last_d = 50.0
    for k in k_series:
        last_d = alpha2 * float(k) + (1.0 - alpha2) * last_d
    current_k = k_series[-1]
    current_d = last_d
    current_j = 3.0 * current_k - 2.0 * current_d
    return {"k": current_k, "d": current_d, "j": current_j}


def obv_with_ma(closes: list, volumes: list, ma_period: int) -> dict:
    p = int(ma_period)
    if len(closes) < p + 1:
        return {"obv": None, "ma": None}
    obv_series: list[float] = [0.0]
    current_obv = 0.0
    for i in range(1, len(closes)):
        c = float(closes[i])
        pc = float(closes[i - 1])
        v = float(volumes[i])
        if c > pc:
            current_obv += v
        elif c < pc:
            current_obv -= v
        obv_series.append(current_obv)
    cur = obv_series[-1]
    s = sum(obv_series[-p:])
    return {"obv": cur, "ma": s / float(p)}


def stoch_rsi(closes: list, rsi_p: int, stoch_p: int, smooth_k: int, smooth_d: int) -> dict:
    p = int(rsi_p)
    if len(closes) < p + 1:
        return {"k": None, "d": None}
    rsi_series: list[float | None] = []
    for i in range(p + 1, len(closes) + 1):
        rsi_series.append(rsi(closes[:i], p))
    s_p = int(stoch_p)
    if len(rsi_series) < s_p:
        return {"k": None, "d": None}
    stoch_series: list[float] = []
    for i in range(s_p - 1, len(rsi_series)):
        sub = rsi_series[i - s_p + 1 : i + 1]
        if any(v is None for v in sub):
            continue
        low = min(float(v) for v in sub if v is not None)
        high = max(float(v) for v in sub if v is not None)
        cur = float(rsi_series[i])
        stoch_series.append(0.0 if high == low else ((cur - low) / (high - low)) * 100.0)
    if not stoch_series:
        return {"k": None, "d": None}
    k_series = ema_series(stoch_series, int(smooth_k))
    d_series = ema_series(k_series, int(smooth_d))
    return {"k": k_series[-1] if k_series else None, "d": d_series[-1] if d_series else None}


def detect_template_param_count(template: str) -> int:
    s = str(template or "")
    it = re.finditer(r"\bn(\d+)\b", s, flags=re.IGNORECASE)
    mx = 0
    for m in it:
        try:
            n = int(m.group(1))
        except Exception:
            continue
        if n > mx:
            mx = n
    return mx


def expand_template(template: str, params: list) -> str:
    s = str(template or "")
    ps = params if isinstance(params, list) else []
    for i in range(1, 13):
        v = ps[i - 1] if i - 1 < len(ps) else None
        if not _is_num(v):
            continue
        fv = float(v)
        rep = (str(fv).rstrip("0").rstrip(".") if fv % 1 else str(int(fv)))
        s = re.sub(rf"\bn{i}\b", rep, s)
    return s


@dataclass(frozen=True)
class Token:
    t: str
    v: object


def tokenize_expr(src: str) -> list[Token]:
    s = str(src or "")
    out: list[Token] = []
    i = 0
    while i < len(s):
        c = s[i]
        if c in " \t\n\r":
            i += 1
            continue
        if c in (">", "<", "=", "!"):
            n = s[i + 1] if i + 1 < len(s) else ""
            if n == "=":
                out.append(Token(f"{c}=", f"{c}="))
                i += 2
                continue
            if c == "=":
                raise ValueError("不支持单独的 =")
            out.append(Token(c, c))
            i += 1
            continue
        if (c.isdigit()) or (c == "." and (i + 1 < len(s) and s[i + 1].isdigit())):
            j = i + 1
            while j < len(s) and (s[j].isdigit() or s[j] == "."):
                j += 1
            out.append(Token("num", float(s[i:j])))
            i = j
            continue
        if (c.isalpha()) or c == "_":
            j = i + 1
            while j < len(s) and (s[j].isalnum() or s[j] == "_"):
                j += 1
            out.append(Token("id", s[i:j]))
            i = j
            continue
        if c in "+-*/(),.":
            out.append(Token(c, c))
            i += 1
            continue
        raise ValueError(f"不支持字符：{c}")
    return out


def parse_expr_tokens(tokens: list[Token]) -> dict:
    pos = 0

    def peek() -> Token | None:
        return tokens[pos] if pos < len(tokens) else None

    def take(t: str | None = None) -> Token:
        nonlocal pos
        cur = peek()
        if cur is None or (t and cur.t != t):
            raise ValueError("表达式解析失败")
        pos += 1
        return cur

    def parse_atom() -> dict:
        cur = peek()
        if cur is None:
            raise ValueError("表达式为空")
        if cur.t == "num":
            take("num")
            return {"k": "num", "v": cur.v}
        if cur.t == "id":
            take("id")
            ident = str(cur.v)
            if peek() and peek().t == "(":
                take("(")
                args: list[dict] = []
                if peek() and peek().t != ")":
                    args.append(parse_add_sub())
                    while peek() and peek().t == ",":
                        take(",")
                        args.append(parse_add_sub())
                take(")")
                return {"k": "call", "name": ident, "args": args}
            return {"k": "id", "name": ident}
        if cur.t == "(":
            take("(")
            e = parse_add_sub()
            take(")")
            return e
        raise ValueError("表达式解析失败")

    def parse_postfix() -> dict:
        node = parse_atom()
        while peek() and peek().t == ".":
            take(".")
            m = str(take("id").v)
            if peek() and peek().t == "(":
                take("(")
                args: list[dict] = [node]
                if peek() and peek().t != ")":
                    args.append(parse_add_sub())
                    while peek() and peek().t == ",":
                        take(",")
                        args.append(parse_add_sub())
                take(")")
                node = {"k": "call", "name": m, "args": args}
            else:
                node = {"k": "member", "obj": node, "prop": m}
        return node

    def parse_unary() -> dict:
        cur = peek()
        if cur and cur.t in ("+", "-"):
            op = take().t
            return {"k": "unary", "op": op, "x": parse_unary()}
        return parse_postfix()

    def parse_mul_div() -> dict:
        node = parse_unary()
        while peek() and peek().t in ("*", "/"):
            op = take().t
            node = {"k": "bin", "op": op, "a": node, "b": parse_unary()}
        return node

    def parse_add_sub() -> dict:
        node = parse_mul_div()
        while peek() and peek().t in ("+", "-"):
            op = take().t
            node = {"k": "bin", "op": op, "a": node, "b": parse_mul_div()}
        return node

    def parse_cmp() -> dict:
        node = parse_add_sub()
        while peek() and peek().t in (">", "<", ">=", "<=", "==", "!="):
            op = take().t
            node = {"k": "cmp", "op": op, "a": node, "b": parse_add_sub()}
        return node

    ast = parse_cmp()
    if pos != len(tokens):
        raise ValueError("表达式解析失败")
    return ast


def _v_scalar(x) -> dict:
    return {"t": "scalar", "v": float(x) if _is_num(x) else None}


def _v_series(arr: list) -> dict:
    return {"t": "series", "v": arr if isinstance(arr, list) else []}


def _is_series(v: dict | None) -> bool:
    return bool(v) and v.get("t") == "series"


def _scalar_from(v: dict | None) -> float | None:
    if not v:
        return None
    if v.get("t") == "scalar":
        return v.get("v")
    if v.get("t") == "series":
        return last_non_null(v.get("v") or [])
    return None


def _series_from(v: dict | None, len_hint: int) -> list:
    if not v:
        return []
    if v.get("t") == "series":
        return v.get("v") or []
    s = v.get("v") if v.get("t") == "scalar" else None
    n = int(len_hint or 0)
    if n <= 0:
        return []
    return [s] * n


def eval_ast(node: dict, ctx: dict) -> dict:
    k = node.get("k")
    if k == "num":
        return _v_scalar(node.get("v"))
    if k == "id":
        name = str(node.get("name") or "").lower()
        if name in ("open", "high", "low", "close", "volume", "quote_volume"):
            return _v_series(ctx["series"].get(name) or [])
        if name == "quotevolume":
            return _v_series(ctx["series"].get("quote_volume") or [])
        if name == "eps":
            return _v_scalar(1e-12)
        return _v_scalar(None)
    if k == "member":
        return _v_scalar(None)
    if k == "unary":
        x = eval_ast(node.get("x"), ctx)
        if _is_series(x):
            out = []
            for v in x["v"]:
                if not _is_num(v):
                    out.append(None)
                else:
                    out.append(-float(v) if node.get("op") == "-" else float(v))
            return _v_series(out)
        v0 = _scalar_from(x)
        if v0 is None:
            return _v_scalar(None)
        return _v_scalar(-v0 if node.get("op") == "-" else v0)
    if k in ("bin", "cmp"):
        a = eval_ast(node.get("a"), ctx)
        b = eval_ast(node.get("b"), ctx)
        a_is_s = _is_series(a)
        b_is_s = _is_series(b)
        length = len(a["v"]) if a_is_s else len(b["v"]) if b_is_s else 0
        as0 = a["v"] if a_is_s else _series_from(a, length)
        bs0 = b["v"] if b_is_s else _series_from(b, length)
        op = node.get("op")
        if length <= 0:
            av = _scalar_from(a)
            bv = _scalar_from(b)
            if av is None or bv is None:
                return _v_scalar(None)
            if k == "bin":
                if op == "+":
                    return _v_scalar(av + bv)
                if op == "-":
                    return _v_scalar(av - bv)
                if op == "*":
                    return _v_scalar(av * bv)
                if op == "/":
                    return _v_scalar(None if bv == 0 else av / bv)
            else:
                if op == ">":
                    return _v_scalar(1 if av > bv else 0)
                if op == ">=":
                    return _v_scalar(1 if av >= bv else 0)
                if op == "<":
                    return _v_scalar(1 if av < bv else 0)
                if op == "<=":
                    return _v_scalar(1 if av <= bv else 0)
                if op == "==":
                    return _v_scalar(1 if av == bv else 0)
                if op == "!=":
                    return _v_scalar(1 if av != bv else 0)
            return _v_scalar(None)
        out = [None] * length
        for i in range(length):
            av = as0[i]
            bv = bs0[i]
            if not _is_num(av) or not _is_num(bv):
                out[i] = None
                continue
            x = float(av)
            y = float(bv)
            if k == "bin":
                if op == "+":
                    out[i] = x + y
                elif op == "-":
                    out[i] = x - y
                elif op == "*":
                    out[i] = x * y
                elif op == "/":
                    out[i] = None if y == 0 else x / y
            else:
                if op == ">":
                    out[i] = 1 if x > y else 0
                elif op == ">=":
                    out[i] = 1 if x >= y else 0
                elif op == "<":
                    out[i] = 1 if x < y else 0
                elif op == "<=":
                    out[i] = 1 if x <= y else 0
                elif op == "==":
                    out[i] = 1 if x == y else 0
                elif op == "!=":
                    out[i] = 1 if x != y else 0
        return _v_series(out)
    if k == "call":
        name = str(node.get("name") or "").lower()
        args = node.get("args") or []

        def ensure_series(n) -> list:
            v = eval_ast(n, ctx)
            if _is_series(v):
                return v["v"]
            return []

        def ensure_scalar(n) -> float | None:
            return _scalar_from(eval_ast(n, ctx))

        if name == "abs":
            x = eval_ast(args[0], ctx) if args else _v_scalar(None)
            if _is_series(x):
                return _v_series([abs(float(v)) if _is_num(v) else None for v in x["v"]])
            v0 = _scalar_from(x)
            return _v_scalar(None if v0 is None else abs(v0))
        if name == "shift":
            if len(args) < 1:
                return _v_scalar(None)
            series = ensure_series(args[0]) if len(args) >= 2 else (ctx["series"].get("close") or [])
            n = ensure_scalar(args[1]) if len(args) >= 2 else ensure_scalar(args[0])
            if not series or n is None:
                return _v_scalar(None)
            k0 = int(n)
            idx = len(series) - 1 - k0
            if idx < 0 or idx >= len(series):
                return _v_scalar(None)
            return _v_scalar(series[idx])
        if name in ("ma", "sma", "ema", "mean", "std"):
            if len(args) < 1:
                return _v_scalar(None)
            series = ensure_series(args[0]) if len(args) >= 2 else (ctx["series"].get("close") or [])
            w = ensure_scalar(args[1]) if len(args) >= 2 else ensure_scalar(args[0])
            if not series or w is None:
                return _v_scalar(None)
            win = int(w)
            if name == "std":
                return _v_scalar(rolling_std(series, win))
            if name == "ema":
                return _v_scalar(ema(series, win))
            return _v_scalar(sma(series, win))
        if name == "rsi":
            if len(args) < 1:
                return _v_scalar(None)
            series = ensure_series(args[0]) if len(args) >= 2 else (ctx["series"].get("close") or [])
            p = ensure_scalar(args[1]) if len(args) >= 2 else ensure_scalar(args[0])
            if not series or p is None:
                return _v_scalar(None)
            return _v_scalar(rsi(series, int(p)))
        if name == "corr":
            if len(args) < 3:
                return _v_scalar(None)
            x = ensure_series(args[0])
            y = ensure_series(args[1])
            w = ensure_scalar(args[2])
            if not x or not y or w is None:
                return _v_scalar(None)
            return _v_scalar(rolling_corr(x, y, int(w)))
        return _v_scalar(None)
    return _v_scalar(None)


def eval_expression(expr: str, row: dict) -> float | None:
    ast = _compile_expr(expr)
    series = {
        "open": get_series(row, "open"),
        "high": get_series(row, "high"),
        "low": get_series(row, "low"),
        "close": get_series(row, "close"),
        "volume": get_series(row, "volume"),
        "quote_volume": get_series(row, "quote_volume"),
    }
    latest_close = last_non_null(series["close"])
    if latest_close is None:
        c = row.get("close")
        latest_close = float(c) if _is_num(c) else None
    ctx = {"series": series, "latest": {"close": latest_close}}
    try:
        t, v = _eval_expr_scalar(ast, ctx)
    except Exception:
        t, v = ("scalar", None)
    if t == "series":
        return _last_series_value(v if isinstance(v, list) else [])
    return float(v) if _is_num(v) else None


def compare(v: float | None, cmp: str, thr: float) -> bool:
    if v is None or not _is_num(v):
        return False
    if not _is_num(thr):
        return False
    x = float(v)
    t = float(thr)
    if cmp == ">":
        return x > t
    if cmp == ">=":
        return x >= t
    if cmp == "<":
        return x < t
    if cmp == "<=":
        return x <= t
    return False


def compute_builtins(row: dict, params: dict) -> dict:
    closes = get_series(row, "close")
    out: dict[str, float | None] = {}
    market0 = str((row or {}).get("market") or (params or {}).get("market") or "").strip().lower()
    if market0 == "all":
        market0 = str((row or {}).get("market") or "").strip().lower()
    fc = _get_cached_factors(market=market0, symbol=str((row or {}).get("symbol") or "")) if market0 else None
    ps = [params.get("maPeriodClose"), params.get("maFast"), params.get("maSlow")]
    for p in ps:
        if _is_num(p) and int(float(p)) > 0:
            k = f"ma_{int(float(p))}"
            if isinstance(fc, dict) and k in fc:
                out[k] = fc.get(k)
                _mark_factor_key_used(k)
            else:
                out[k] = sma(closes, int(float(p)))
    rp = params.get("rsiPeriod")
    if _is_num(rp) and int(float(rp)) > 0:
        k = f"rsi_{int(float(rp))}"
        if isinstance(fc, dict) and k in fc:
            out[k] = fc.get(k)
            _mark_factor_key_used(k)
        else:
            out[k] = rsi(closes, int(float(rp)))
    return out


def apply_all_filters(rows: list, config: dict) -> dict:
    params = (config or {}).get("params") or {}
    toggles = (config or {}).get("toggles") or {}
    custom_factors = (config or {}).get("customFactors") or []
    market = str(params.get("market") or "all")
    lists0 = (config or {}).get("lists") or {}
    wl0 = (lists0.get("whitelist") if isinstance(lists0, dict) else None) or (config or {}).get("whitelist") or []
    bl0 = (lists0.get("blacklist") if isinstance(lists0, dict) else None) or (config or {}).get("blacklist") or []
    whitelist = {str(x).strip().upper() for x in (wl0 or []) if str(x).strip()}
    blacklist = {str(x).strip().upper() for x in (bl0 or []) if str(x).strip()}

    def base_symbol(sym: str) -> str:
        s = str(sym or "").strip().upper()
        if s.endswith("-USDT"):
            return s[:-5]
        if s.endswith("USDT"):
            return s[:-4]
        if "-" in s:
            return s.split("-", 1)[0]
        return s

    selected: list[dict] = []
    filtered_out = 0
    expr_errors = 0
    for r in rows or []:
        if market != "all" and str(r.get("market")) != market:
            continue
        sym0 = str(r.get("symbol") or "")
        bs0 = base_symbol(sym0)
        if whitelist and (sym0.upper() not in whitelist) and (bs0 not in whitelist):
            filtered_out += 1
            continue
        if blacklist and ((sym0.upper() in blacklist) or (bs0 in blacklist)):
            filtered_out += 1
            continue
        r["_builtins"] = compute_builtins(r, params)
        r["_expr"] = {}

        closes = get_series(r, "close")
        highs = get_series(r, "high")
        lows = get_series(r, "low")
        volumes = get_series(r, "volume")
        last_close = float(r.get("close") or 0.0)
        market0 = str((r or {}).get("market") or "").strip().lower()
        fc = _get_cached_factors(market=market0, symbol=sym0) if market0 else None

        if toggles.get("condCloseMa"):
            ma_v = r["_builtins"].get(f"ma_{int(params.get('maPeriodClose') or 0)}")
            if ma_v is None or not (last_close > float(ma_v)):
                filtered_out += 1
                continue

        if toggles.get("condMa"):
            ma_f = r["_builtins"].get(f"ma_{int(params.get('maFast') or 0)}")
            ma_s = r["_builtins"].get(f"ma_{int(params.get('maSlow') or 0)}")
            if ma_f is None or ma_s is None or not (float(ma_f) > float(ma_s)):
                filtered_out += 1
                continue

        if toggles.get("condRsi"):
            rv = r["_builtins"].get(f"rsi_{int(params.get('rsiPeriod') or 0)}")
            thr = params.get("rsiThreshold")
            if rv is None or not _is_num(thr) or not (float(rv) > float(thr)):
                filtered_out += 1
                continue

        if toggles.get("condEma"):
            ep = int(params.get("emaPeriod") or 0)
            ev = (fc.get(f"ema_{ep}") if isinstance(fc, dict) else None) if ep > 0 else None
            if ev is None:
                ev = ema(closes, ep)
            else:
                _mark_factor_key_used(f"ema_{ep}")
            r["_builtins"]["ema"] = ev
            if ev is None or not (last_close > float(ev)):
                filtered_out += 1
                continue

        if toggles.get("condBollUp"):
            bp = int(params.get("bollPeriod") or 0)
            ma0 = (fc.get(f"ma_{bp}") if isinstance(fc, dict) else None) if bp > 0 else None
            std0 = (fc.get(f"std_{bp}") if isinstance(fc, dict) else None) if bp > 0 else None
            if ma0 is None:
                ma0 = sma(closes, bp)
            else:
                _mark_factor_key_used(f"ma_{bp}")
            if std0 is None:
                std0 = rolling_std(closes, bp)
            else:
                _mark_factor_key_used(f"std_{bp}")
            if ma0 is None or std0 is None:
                filtered_out += 1
                continue
            if not _is_num(params.get("bollStd")):
                filtered_out += 1
                continue
            boll_up = float(ma0) + float(params.get("bollStd")) * float(std0)
            r["_builtins"]["boll_up"] = boll_up
            if not (last_close > boll_up):
                filtered_out += 1
                continue

        if toggles.get("condBollDown"):
            bp = int(params.get("bollDownPeriod") or 0)
            ma0 = (fc.get(f"ma_{bp}") if isinstance(fc, dict) else None) if bp > 0 else None
            std0 = (fc.get(f"std_{bp}") if isinstance(fc, dict) else None) if bp > 0 else None
            if ma0 is None:
                ma0 = sma(closes, bp)
            else:
                _mark_factor_key_used(f"ma_{bp}")
            if std0 is None:
                std0 = rolling_std(closes, bp)
            else:
                _mark_factor_key_used(f"std_{bp}")
            if ma0 is None or std0 is None:
                filtered_out += 1
                continue
            if not _is_num(params.get("bollDownStd")):
                filtered_out += 1
                continue
            boll_down = float(ma0) - float(params.get("bollDownStd")) * float(std0)
            r["_builtins"]["boll_down"] = boll_down
            if not (last_close < boll_down):
                filtered_out += 1
                continue

        if toggles.get("condSuper"):
            ap = int(params.get("superAtrPeriod") or 0)
            mult = float(params.get("superMult") or 0.0)
            st = None
            if isinstance(fc, dict) and ap == 10 and mult == 3.0:
                st = fc.get("supertrend")
            if st is None:
                st = supertrend(highs, lows, closes, ap, mult)
            else:
                _mark_factor_key_used("supertrend")
            r["_builtins"]["supertrend"] = st
            if st is None or not (last_close > float(st)):
                filtered_out += 1
                continue

        if toggles.get("condKdj"):
            kn = int(params.get("kdjN") or 0)
            km1 = int(params.get("kdjM1") or 0)
            km2 = int(params.get("kdjM2") or 0)
            out = None
            if isinstance(fc, dict) and kn == 9 and km1 == 3 and km2 == 3:
                out = {"k": fc.get("kdj_k"), "d": fc.get("kdj_d"), "j": fc.get("kdj_j")}
            if not isinstance(out, dict):
                out = kdj(highs, lows, closes, kn, km1, km2)
            else:
                _mark_factor_key_used("kdj_k")
                _mark_factor_key_used("kdj_d")
            k0 = out.get("k")
            d0 = out.get("d")
            r["_builtins"]["kdj_k"] = k0
            r["_builtins"]["kdj_d"] = d0
            r["_builtins"]["kdj_j"] = out.get("j")
            if k0 is None or d0 is None or not (float(k0) > float(d0)):
                filtered_out += 1
                continue

        if toggles.get("condObv"):
            op = int(params.get("obvMaPeriod") or 0)
            out = None
            if isinstance(fc, dict) and op == 20:
                out = {"obv": fc.get("obv"), "ma": fc.get("obv_ma")}
            if not isinstance(out, dict):
                out = obv_with_ma(closes, volumes, op)
            else:
                _mark_factor_key_used("obv")
                _mark_factor_key_used("obv_ma")
            ov = out.get("obv")
            om = out.get("ma")
            r["_builtins"]["obv"] = ov
            r["_builtins"]["obv_ma"] = om
            if ov is None or om is None or not (float(ov) > float(om)):
                filtered_out += 1
                continue

        if toggles.get("condStochRsi"):
            sp = int(params.get("stochRsiP") or 0)
            sk = int(params.get("stochRsiK") or 0)
            ssk = int(params.get("stochRsiSmK") or 0)
            ssd = int(params.get("stochRsiSmD") or 0)
            out = None
            if isinstance(fc, dict) and sp == 14 and sk == 14 and ssk == 3 and ssd == 3:
                out = {"k": fc.get("stoch_rsi_k"), "d": fc.get("stoch_rsi_d")}
            if not isinstance(out, dict):
                out = stoch_rsi(closes, sp, sk, ssk, ssd)
            else:
                _mark_factor_key_used("stoch_rsi_k")
                _mark_factor_key_used("stoch_rsi_d")
            k0 = out.get("k")
            d0 = out.get("d")
            r["_builtins"]["stoch_rsi_k"] = k0
            r["_builtins"]["stoch_rsi_d"] = d0
            if k0 is None or d0 is None or not (float(k0) > float(d0)):
                filtered_out += 1
                continue

        expr_fail = False
        for f in custom_factors:
            try:
                template = str((f or {}).get("template") or (f or {}).get("expr") or "")
                expr = expand_template(template, (f or {}).get("params") or [])
                v = eval_expression(expr, r)
                fid = (f or {}).get("id")
                if fid is not None:
                    r["_expr"][fid] = v
                if not (f or {}).get("enabled"):
                    continue
                if (f or {}).get("thresholdEnabled"):
                    if not compare(v, str((f or {}).get("cmp") or ">="), float((f or {}).get("threshold") or 0.0)):
                        expr_fail = True
                        break
                else:
                    if v is None or (not _is_num(v)) or float(v) == 0.0:
                        expr_fail = True
                        break
            except Exception:
                expr_errors += 1
                expr_fail = True
                break
        if expr_fail:
            filtered_out += 1
            continue
        selected.append(r)

    return {"selected": selected, "filteredOut": filtered_out, "exprErrors": expr_errors}


def sort_rows(rows: list[dict], config: dict) -> tuple[list[dict], str]:
    sort_cfg = (config or {}).get("sort") or {}
    key = str(sort_cfg.get("key") or "close")
    order = str(sort_cfg.get("order") or "desc")
    dir0 = 1 if order == "asc" else -1
    params = (config or {}).get("params") or {}

    def get_value(r: dict):
        if key == "close":
            v = r.get("close")
            return float(v) if _is_num(v) else None
        if key.startswith("ma_") or key.startswith("rsi_"):
            return r.get("_builtins", {}).get(key)
        if key == "ema":
            b = (r.get("_builtins") or {}) if isinstance(r.get("_builtins"), dict) else {}
            if "ema" in b:
                return b.get("ema")
            ep = int(params.get("emaPeriod") or 0)
            fc = _get_cached_factors(market=str(r.get("market") or "").strip().lower(), symbol=str(r.get("symbol") or ""))
            v = (fc.get(f"ema_{ep}") if isinstance(fc, dict) else None) if ep > 0 else None
            if v is None:
                v = ema(get_series(r, "close"), ep)
            else:
                _mark_factor_key_used(f"ema_{ep}")
            b["ema"] = v
            r["_builtins"] = b
            return v
        if key == "boll_up":
            b = (r.get("_builtins") or {}) if isinstance(r.get("_builtins"), dict) else {}
            if "boll_up" in b:
                return b.get("boll_up")
            bp = int(params.get("bollPeriod") or 0)
            fc = _get_cached_factors(market=str(r.get("market") or "").strip().lower(), symbol=str(r.get("symbol") or ""))
            ma0 = (fc.get(f"ma_{bp}") if isinstance(fc, dict) else None) if bp > 0 else None
            std0 = (fc.get(f"std_{bp}") if isinstance(fc, dict) else None) if bp > 0 else None
            if ma0 is None:
                ma0 = sma(get_series(r, "close"), bp)
            else:
                _mark_factor_key_used(f"ma_{bp}")
            if std0 is None:
                std0 = rolling_std(get_series(r, "close"), bp)
            else:
                _mark_factor_key_used(f"std_{bp}")
            if ma0 is None or std0 is None or not _is_num(params.get("bollStd")):
                return None
            v = float(ma0) + float(params.get("bollStd")) * float(std0)
            b["boll_up"] = v
            r["_builtins"] = b
            return v
        if key == "boll_down":
            b = (r.get("_builtins") or {}) if isinstance(r.get("_builtins"), dict) else {}
            if "boll_down" in b:
                return b.get("boll_down")
            bp = int(params.get("bollDownPeriod") or 0)
            fc = _get_cached_factors(market=str(r.get("market") or "").strip().lower(), symbol=str(r.get("symbol") or ""))
            ma0 = (fc.get(f"ma_{bp}") if isinstance(fc, dict) else None) if bp > 0 else None
            std0 = (fc.get(f"std_{bp}") if isinstance(fc, dict) else None) if bp > 0 else None
            if ma0 is None:
                ma0 = sma(get_series(r, "close"), bp)
            else:
                _mark_factor_key_used(f"ma_{bp}")
            if std0 is None:
                std0 = rolling_std(get_series(r, "close"), bp)
            else:
                _mark_factor_key_used(f"std_{bp}")
            if ma0 is None or std0 is None or not _is_num(params.get("bollDownStd")):
                return None
            v = float(ma0) - float(params.get("bollDownStd")) * float(std0)
            b["boll_down"] = v
            r["_builtins"] = b
            return v
        if key == "supertrend":
            b = (r.get("_builtins") or {}) if isinstance(r.get("_builtins"), dict) else {}
            if "supertrend" in b:
                return b.get("supertrend")
            ap = int(params.get("superAtrPeriod") or 0)
            mult = float(params.get("superMult") or 0.0)
            fc = _get_cached_factors(market=str(r.get("market") or "").strip().lower(), symbol=str(r.get("symbol") or ""))
            v = (fc.get("supertrend") if isinstance(fc, dict) and ap == 10 and mult == 3.0 else None)
            if v is None:
                v = supertrend(get_series(r, "high"), get_series(r, "low"), get_series(r, "close"), ap, mult)
            else:
                _mark_factor_key_used("supertrend")
            b["supertrend"] = v
            r["_builtins"] = b
            return v
        if key.startswith("expr_"):
            fid = key[len("expr_") :]
            return (r.get("_expr") or {}).get(fid)
        v = r.get(key)
        if isinstance(v, str):
            return v
        return float(v) if _is_num(v) else None

    def sort_key_fn(r: dict):
        v = get_value(r)
        if v is None:
            return (1, 0)
        if isinstance(v, str):
            return (0, v)
        return (0, float(v))

    sorted_rows = sorted(rows, key=sort_key_fn, reverse=(dir0 == -1))
    return sorted_rows, key


def assign_rank(rows: list[dict], sort_key: str, config: dict) -> None:
    params = (config or {}).get("params") or {}

    def get_value(r: dict):
        if sort_key == "close":
            v = r.get("close")
            return float(v) if _is_num(v) else None
        if sort_key.startswith("ma_") or sort_key.startswith("rsi_"):
            return r.get("_builtins", {}).get(sort_key)
        if sort_key == "ema":
            b = (r.get("_builtins") or {}) if isinstance(r.get("_builtins"), dict) else {}
            if "ema" in b:
                return b.get("ema")
            ep = int(params.get("emaPeriod") or 0)
            fc = _get_cached_factors(market=str(r.get("market") or "").strip().lower(), symbol=str(r.get("symbol") or ""))
            v = (fc.get(f"ema_{ep}") if isinstance(fc, dict) else None) if ep > 0 else None
            if v is None:
                v = ema(get_series(r, "close"), ep)
            b["ema"] = v
            r["_builtins"] = b
            return v
        if sort_key == "ema":
            return ema(get_series(r, "close"), int(params.get("emaPeriod") or 0))
        if sort_key.startswith("expr_"):
            fid = sort_key[len("expr_") :]
            return (r.get("_expr") or {}).get(fid)
        v = r.get(sort_key)
        return float(v) if _is_num(v) else None

    numeric = [r for r in rows if _is_num(get_value(r))]
    num_set = set(map(id, numeric))
    i = 1
    for r in rows:
        if id(r) in num_set:
            r["_rank"] = i
            i += 1
        else:
            r["_rank"] = ""
