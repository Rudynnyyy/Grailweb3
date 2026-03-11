from __future__ import annotations

import argparse
import json
import os
import pickle
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import timezone
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd


_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _repo_root() -> Path:
    return _REPO_ROOT


def _decode_bytes(raw: bytes) -> str:
    for enc in ("gbk", "utf-8", "latin-1", "cp1252"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


def _read_tail_text(path: Path, *, max_lines: int) -> list[str]:
    try:
        with path.open("rb") as f:
            f.seek(0, os.SEEK_END)
            end = f.tell()
            if end <= 0:
                return []
            block = 65536
            buf = b""
            pos = end
            while pos > 0 and buf.count(b"\n") < max_lines + 2:
                step = block if pos >= block else pos
                pos -= step
                f.seek(pos)
                buf = f.read(step) + buf
        text = _decode_bytes(buf)
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        if len(lines) > max_lines:
            lines = lines[-max_lines:]
        return lines
    except Exception:
        return []


def _read_header_line(path: Path, *, max_bytes: int = 65536) -> str:
    try:
        with path.open("rb") as f:
            raw = f.read(max_bytes)
        text = _decode_bytes(raw)
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        if not lines:
            return ""
        for ln in lines[:200]:
            s = ln.lstrip("\ufeff").strip().lower()
            if "candle_begin_time" in s:
                return ln
            if "dt" in s and "," in s:
                return ln
        return ""
    except Exception:
        return ""


def read_csv_tail(path: Path, *, tail: int, extra: int = 300) -> pd.DataFrame:
    try:
        from apps.crypto_screener.app.series_source import read_merge_csv_tail

        df = read_merge_csv_tail(path, tail=int(tail), extra=int(extra))
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    except Exception:
        header = _read_header_line(path, max_bytes=262144)
        lines = _read_tail_text(path, max_lines=max(2000, int(tail) + int(extra)))
        if not header or not lines:
            return pd.DataFrame()
        header_clean = header.strip().lstrip("\ufeff").strip()
        body: list[str] = []
        for ln in lines:
            ln0 = ln.strip()
            if not ln0:
                continue
            if ln0.lstrip("\ufeff").strip() == header_clean:
                continue
            if "candle_begin_time" in ln0 or ln0.startswith("数据由币安API获取"):
                continue
            body.append(ln0)
        csv_text = header + "\n" + "\n".join(body)
        try:
            df = pd.read_csv(StringIO(csv_text), sep=None, engine="python", on_bad_lines="skip")
        except TypeError:
            df = pd.read_csv(StringIO(csv_text), sep=None, engine="python")
        try:
            df.columns = [str(c).lstrip("\ufeff").strip() for c in list(df.columns)]
        except Exception:
            pass
        return df


def _ensure_dt(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    col = "candle_begin_time" if "candle_begin_time" in df.columns else ("dt" if "dt" in df.columns else "")
    if not col:
        return pd.DataFrame()
    s = df[col]
    try:
        dt = pd.to_datetime(s, errors="coerce", utc=True, format="mixed")
    except TypeError:
        dt = pd.to_datetime(s, errors="coerce", utc=True)
    if dt.notna().sum() < max(3, int(len(dt) * 0.2)):
        xs = pd.to_numeric(s, errors="coerce")
        if xs.notna().sum() >= max(3, int(len(xs) * 0.8)):
            dt_ms = pd.to_datetime(xs, errors="coerce", utc=True, unit="ms")
            dt_s = pd.to_datetime(xs, errors="coerce", utc=True, unit="s")
            if dt_ms.notna().sum() >= dt.notna().sum():
                dt = dt_ms
            if dt_s.notna().sum() > dt.notna().sum():
                dt = dt_s
    df = df.copy()
    df[col] = dt
    df = df.dropna(subset=[col]).sort_values(col)
    if col != "candle_begin_time":
        df = df.rename(columns={col: "candle_begin_time"})
    return df


def _default_merge_dir(repo_root: Path, market: str) -> Path:
    if str(market).lower() == "spot":
        env = os.environ.get("QC_MERGE_SPOT_PATH") or os.environ.get("QC_SCREENER_FALLBACK_SPOT_DIR")
        return Path(env) if env else (repo_root / "数据获取" / "data" / "spot_lin")
    env = os.environ.get("QC_MERGE_SWAP_PATH") or os.environ.get("QC_SCREENER_FALLBACK_SWAP_DIR")
    return Path(env) if env else (repo_root / "数据获取" / "data" / "swap_lin")


def _default_data_center_dir(repo_root: Path, market: str) -> Path:
    root = Path(os.environ.get("QC_DATA_CENTER_ROOT") or str(repo_root / "数据获取" / "data" / "data_center"))
    return root / "kline" / ("spot" if str(market).lower() == "spot" else "swap")


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


def _pick_csv_path(base_dirs: list[Path], symbol: str) -> Path | None:
    cands = _symbol_candidates(symbol)
    found: list[Path] = []
    for base in list(base_dirs or []):
        base_r = base.resolve()
        for sym in cands:
            p = (base / f"{sym}.csv").resolve()
            if base_r not in p.parents and p != base_r:
                continue
            if p.exists() and p.is_file():
                found.append(p)
    return found[0] if found else None


def _to_float_list(arr) -> list[float | None]:
    out: list[float | None] = []
    s = pd.to_numeric(arr, errors="coerce")
    for v in s.tolist():
        if v is None or (isinstance(v, float) and pd.isna(v)):
            out.append(None)
        else:
            try:
                out.append(float(v))
            except Exception:
                out.append(None)
    return out


def _df_to_series(df: pd.DataFrame, *, tail: int) -> tuple[list[str], dict[str, list[float | None]]]:
    if df is None or df.empty:
        return [], {}
    df = _ensure_dt(df)
    if df.empty:
        return [], {}
    df = df.tail(max(1, int(tail))).copy()
    for col in ("open", "high", "low", "close"):
        if col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce")
            df[col] = s.ffill().bfill()
    for col in ("volume", "quote_volume", "quotevolume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    dt_list: list[str] = []
    for x in df["candle_begin_time"].tolist():
        try:
            dt_list.append(x.astimezone(timezone.utc).isoformat())
        except Exception:
            dt_list.append(pd.to_datetime(x, utc=True, errors="coerce").to_pydatetime().replace(tzinfo=timezone.utc).isoformat())
    series: dict[str, list[float | None]] = {}
    for col in ("open", "high", "low", "close", "volume", "quote_volume"):
        if col in df.columns:
            series[col] = _to_float_list(df[col])
        elif col == "quote_volume" and "quotevolume" in df.columns:
            series[col] = _to_float_list(df["quotevolume"])
    return dt_list, series


def _merge_two_tails(*, base_df: pd.DataFrame, dc_df: pd.DataFrame, tail: int) -> pd.DataFrame:
    if base_df is None:
        base_df = pd.DataFrame()
    if dc_df is None:
        dc_df = pd.DataFrame()
    if base_df.empty and dc_df.empty:
        return pd.DataFrame()
    if base_df.empty:
        return _ensure_dt(dc_df).tail(int(tail)).copy()
    if dc_df.empty:
        return _ensure_dt(base_df).tail(int(tail)).copy()
    a = _ensure_dt(base_df)
    b = _ensure_dt(dc_df)
    if a.empty:
        return b.tail(int(tail)).copy()
    if b.empty:
        return a.tail(int(tail)).copy()
    merged = pd.concat([a, b], ignore_index=True)
    merged = merged.drop_duplicates(subset=["candle_begin_time"], keep="last")
    merged = merged.sort_values("candle_begin_time")
    return merged.tail(int(tail)).copy()


def _parse_int_list(env_val: str, default_vals: list[int]) -> list[int]:
    s = str(env_val or "").strip()
    if not s:
        return list(default_vals)
    out: list[int] = []
    for part in s.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            v = int(float(part))
        except Exception:
            continue
        if v > 0 and v not in out:
            out.append(v)
    return out if out else list(default_vals)


def _parse_float_pair(env_val: str, default_a: int, default_b: float) -> tuple[int, float]:
    s = str(env_val or "").strip()
    if not s:
        return int(default_a), float(default_b)
    parts = [p.strip() for p in s.replace(";", ",").split(",") if p.strip()]
    if len(parts) < 2:
        return int(default_a), float(default_b)
    try:
        a = int(float(parts[0]))
    except Exception:
        a = int(default_a)
    try:
        b = float(parts[1])
    except Exception:
        b = float(default_b)
    return max(1, a), float(b)


def _parse_int_triple(env_val: str, a0: int, b0: int, c0: int) -> tuple[int, int, int]:
    s = str(env_val or "").strip()
    if not s:
        return int(a0), int(b0), int(c0)
    parts = [p.strip() for p in s.replace(";", ",").split(",") if p.strip()]
    if len(parts) < 3:
        return int(a0), int(b0), int(c0)
    out = []
    for i, d in enumerate((a0, b0, c0)):
        try:
            out.append(int(float(parts[i])))
        except Exception:
            out.append(int(d))
    return max(1, out[0]), max(1, out[1]), max(1, out[2])


def _parse_int_quad(env_val: str, a0: int, b0: int, c0: int, d0: int) -> tuple[int, int, int, int]:
    s = str(env_val or "").strip()
    if not s:
        return int(a0), int(b0), int(c0), int(d0)
    parts = [p.strip() for p in s.replace(";", ",").split(",") if p.strip()]
    if len(parts) < 4:
        return int(a0), int(b0), int(c0), int(d0)
    out = []
    for i, d in enumerate((a0, b0, c0, d0)):
        try:
            out.append(int(float(parts[i])))
        except Exception:
            out.append(int(d))
    return max(1, out[0]), max(1, out[1]), max(1, out[2]), max(1, out[3])


def _compute_factors(*, series: dict[str, list[float | None]]) -> dict[str, float | None]:
    from apps.crypto_screener.app import filter_engine

    closes = series.get("close") or []
    highs = series.get("high") or []
    lows = series.get("low") or []
    vols = series.get("volume") or []

    ma_ws = _parse_int_list(os.environ.get("QC_FACTOR_CACHE_MA_WINDOWS") or "", [10, 20])
    rsi_ps = _parse_int_list(os.environ.get("QC_FACTOR_CACHE_RSI_PERIODS") or "", [14])
    ema_ps = _parse_int_list(os.environ.get("QC_FACTOR_CACHE_EMA_PERIODS") or "", [20])
    std_ws = _parse_int_list(os.environ.get("QC_FACTOR_CACHE_STD_WINDOWS") or "", [20])
    obv_ps = _parse_int_list(os.environ.get("QC_FACTOR_CACHE_OBV_MA_PERIODS") or "", [20])
    st_atr, st_mult = _parse_float_pair(os.environ.get("QC_FACTOR_CACHE_SUPER") or "", 10, 3.0)
    kdj_n, kdj_m1, kdj_m2 = _parse_int_triple(os.environ.get("QC_FACTOR_CACHE_KDJ") or "", 9, 3, 3)
    sr_p, sr_k, sr_sk, sr_sd = _parse_int_quad(os.environ.get("QC_FACTOR_CACHE_STOCH_RSI") or "", 14, 14, 3, 3)

    out: dict[str, float | None] = {}
    for w in ma_ws:
        out[f"ma_{w}"] = filter_engine.sma(closes, int(w))
    for p in rsi_ps:
        out[f"rsi_{p}"] = filter_engine.rsi(closes, int(p))
    for p in ema_ps:
        out[f"ema_{p}"] = filter_engine.ema(closes, int(p))
        if int(p) == 20 and "ema" not in out:
            out["ema"] = out[f"ema_{p}"]
    for w in std_ws:
        out[f"std_{w}"] = filter_engine.rolling_std(closes, int(w))
    for p in obv_ps:
        x = filter_engine.obv_with_ma(closes, vols, int(p))
        if isinstance(x, dict):
            out[f"obv_{p}"] = x.get("obv")
            out[f"obv_ma_{p}"] = x.get("ma")
            if int(p) == 20:
                out.setdefault("obv", x.get("obv"))
                out.setdefault("obv_ma", x.get("ma"))
    try:
        stv = filter_engine.supertrend(highs, lows, closes, int(st_atr), float(st_mult))
    except Exception:
        stv = None
    out[f"supertrend_{int(st_atr)}_{float(st_mult)}"] = stv
    if int(st_atr) == 10 and float(st_mult) == 3.0:
        out.setdefault("supertrend", stv)
    try:
        kdjv = filter_engine.kdj(highs, lows, closes, int(kdj_n), int(kdj_m1), int(kdj_m2))
    except Exception:
        kdjv = {}
    if isinstance(kdjv, dict):
        out[f"kdj_k_{int(kdj_n)}_{int(kdj_m1)}_{int(kdj_m2)}"] = kdjv.get("k")
        out[f"kdj_d_{int(kdj_n)}_{int(kdj_m1)}_{int(kdj_m2)}"] = kdjv.get("d")
        out[f"kdj_j_{int(kdj_n)}_{int(kdj_m1)}_{int(kdj_m2)}"] = kdjv.get("j")
        if int(kdj_n) == 9 and int(kdj_m1) == 3 and int(kdj_m2) == 3:
            out.setdefault("kdj_k", kdjv.get("k"))
            out.setdefault("kdj_d", kdjv.get("d"))
            out.setdefault("kdj_j", kdjv.get("j"))
    try:
        srv = filter_engine.stoch_rsi(closes, int(sr_p), int(sr_k), int(sr_sk), int(sr_sd))
    except Exception:
        srv = {}
    if isinstance(srv, dict):
        out[f"stoch_rsi_k_{int(sr_p)}_{int(sr_k)}_{int(sr_sk)}_{int(sr_sd)}"] = srv.get("k")
        out[f"stoch_rsi_d_{int(sr_p)}_{int(sr_k)}_{int(sr_sk)}_{int(sr_sd)}"] = srv.get("d")
        if int(sr_p) == 14 and int(sr_k) == 14 and int(sr_sk) == 3 and int(sr_sd) == 3:
            out.setdefault("stoch_rsi_k", srv.get("k"))
            out.setdefault("stoch_rsi_d", srv.get("d"))
    return out


def _load_one_symbol(
    *,
    market: str,
    symbol: str,
    merge_dir: Path,
    dc_dir: Path,
    tail: int,
    update_tail: int,
    prev: dict | None,
) -> tuple[str, dict | None, dict | None]:
    merge_path = _pick_csv_path([merge_dir], symbol)
    dc_path = _pick_csv_path([dc_dir], symbol)
    src = {"merge_mtime_ns": None, "dc_mtime_ns": None, "merge_size": None, "dc_size": None}
    try:
        if merge_path is not None and merge_path.exists():
            st = merge_path.stat()
            src["merge_mtime_ns"] = int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9)))
            src["merge_size"] = int(st.st_size)
    except Exception:
        pass
    try:
        if dc_path is not None and dc_path.exists():
            st = dc_path.stat()
            src["dc_mtime_ns"] = int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9)))
            src["dc_size"] = int(st.st_size)
    except Exception:
        pass
    last_dt = None
    try:
        if isinstance(prev, dict):
            dt0 = prev.get("dt")
            if isinstance(dt0, list) and dt0:
                last_dt = pd.to_datetime(str(dt0[-1]), utc=True, errors="coerce")
                if pd.isna(last_dt):
                    last_dt = None
    except Exception:
        last_dt = None

    if merge_path is None and dc_path is None:
        return str(symbol).upper(), None, None

    if last_dt is None:
        base_df = read_csv_tail(merge_path, tail=int(tail), extra=600) if merge_path else pd.DataFrame()
        dc_df = read_csv_tail(dc_path, tail=min(200, int(tail)), extra=200) if dc_path else pd.DataFrame()
        df = _merge_two_tails(base_df=base_df, dc_df=dc_df, tail=int(tail))
        dt_list, series = _df_to_series(df, tail=int(tail))
        if not dt_list:
            return str(symbol).upper(), None, None
        series_rec = {"dt": dt_list, "series": series, "src": src}
        return str(symbol).upper(), series_rec, _compute_factors(series=series)

    base_df = read_csv_tail(merge_path, tail=int(update_tail), extra=200) if merge_path else pd.DataFrame()
    dc_df = read_csv_tail(dc_path, tail=min(200, int(update_tail)), extra=200) if dc_path else pd.DataFrame()
    df_new = _merge_two_tails(base_df=base_df, dc_df=dc_df, tail=int(update_tail))
    if df_new.empty:
        prev1 = prev if isinstance(prev, dict) else {}
        if isinstance(prev1, dict):
            prev1 = dict(prev1)
            prev1["src"] = src
        return str(symbol).upper(), prev1, _compute_factors(series=(prev1 or {}).get("series") or {})
    df_new = _ensure_dt(df_new)
    df_new = df_new[df_new["candle_begin_time"] > last_dt]
    if df_new.empty:
        prev1 = prev if isinstance(prev, dict) else {}
        if isinstance(prev1, dict):
            prev1 = dict(prev1)
            prev1["src"] = src
        return str(symbol).upper(), prev1, _compute_factors(series=(prev1 or {}).get("series") or {})
    dt_new, series_new = _df_to_series(df_new, tail=int(update_tail))
    if not dt_new:
        prev1 = prev if isinstance(prev, dict) else {}
        if isinstance(prev1, dict):
            prev1 = dict(prev1)
            prev1["src"] = src
        return str(symbol).upper(), prev1, _compute_factors(series=(prev1 or {}).get("series") or {})
    dt_old = (prev or {}).get("dt") if isinstance(prev, dict) else []
    series_old = (prev or {}).get("series") if isinstance(prev, dict) else {}
    if not isinstance(dt_old, list):
        dt_old = []
    if not isinstance(series_old, dict):
        series_old = {}

    dt_all = list(dt_old) + list(dt_new)
    keep_cols = ("open", "high", "low", "close", "volume", "quote_volume")
    series_all: dict[str, list[float | None]] = {}
    for col in keep_cols:
        a = series_old.get(col) if isinstance(series_old.get(col), list) else []
        b = series_new.get(col) if isinstance(series_new.get(col), list) else []
        series_all[col] = list(a) + list(b)

    df_tmp = pd.DataFrame({"dt": dt_all})
    df_tmp["dt"] = pd.to_datetime(df_tmp["dt"], utc=True, errors="coerce")
    for col in keep_cols:
        df_tmp[col] = series_all[col]
    df_tmp = df_tmp.dropna(subset=["dt"]).sort_values("dt")
    df_tmp = df_tmp.drop_duplicates(subset=["dt"], keep="last").tail(int(tail)).copy()
    dt_out = [x.astimezone(timezone.utc).isoformat() for x in df_tmp["dt"].tolist()]
    series_out: dict[str, list[float | None]] = {}
    for col in keep_cols:
        series_out[col] = _to_float_list(df_tmp[col])
    series_rec = {"dt": dt_out, "series": series_out, "src": src}
    return str(symbol).upper(), series_rec, _compute_factors(series=series_out)


def _cache_root(repo_root: Path) -> Path:
    env = (os.environ.get("QC_PKL_CACHE_ROOT") or "").strip()
    if env:
        return Path(env)
    out_root = (os.environ.get("QC_PREPROCESS_OUT_ROOT") or "").strip()
    if out_root:
        return Path(out_root) / "pkl_cache"
    return (repo_root / "数据获取" / "data" / "preprocessed_hourly") / "pkl_cache"


def _atomic_pickle_dump(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)
    os.replace(tmp, path)


def _maybe_load_pickle(path: Path) -> Any:
    try:
        with path.open("rb") as f:
            return pickle.load(f)
    except Exception:
        return None


def build_market_cache(*, market: str, tail: int, symbols_limit: int, workers: int, incremental: bool) -> dict:
    repo_root = _repo_root()
    merge_dir = _default_merge_dir(repo_root, market)
    dc_dir = _default_data_center_dir(repo_root, market)
    out_root = _cache_root(repo_root)
    series_path = out_root / f"series_{str(market).lower()}.pkl"
    factors_path = out_root / f"factors_{str(market).lower()}.pkl"

    prev_series = _maybe_load_pickle(series_path) if incremental else None
    prev_map = None
    prev_tail = None
    if isinstance(prev_series, dict):
        prev_tail = prev_series.get("tail")
        prev_map = prev_series.get("symbols") if isinstance(prev_series.get("symbols"), dict) else None
    if not isinstance(prev_map, dict) or int(prev_tail or 0) != int(tail):
        prev_map = {}

    prev_factors = _maybe_load_pickle(factors_path) if incremental else None
    prev_factors_map = None
    if isinstance(prev_factors, dict):
        prev_factors_map = prev_factors.get("factors") if isinstance(prev_factors.get("factors"), dict) else None
    if not isinstance(prev_factors_map, dict):
        prev_factors_map = {}

    update_tail = int(os.environ.get("QC_PKL_CACHE_UPDATE_TAIL") or "450")
    update_tail = max(120, min(1200, update_tail))

    symbols: list[str] = []
    for d in (merge_dir, dc_dir):
        if not d.exists():
            continue
        for p in sorted(d.glob("*.csv")):
            sym = str(p.stem or "").strip().upper()
            if sym and sym not in symbols:
                symbols.append(sym)
            if symbols_limit > 0 and len(symbols) >= symbols_limit:
                break
        if symbols_limit > 0 and len(symbols) >= symbols_limit:
            break

    started = time.time()
    out_series: dict[str, dict] = {}
    out_factors: dict[str, dict] = {}
    ok = 0
    reused = 0
    reused_by_dt = 0

    def _stat_ns(p: Path) -> int:
        try:
            st = p.stat()
            return int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9)))
        except Exception:
            return -1

    for sym in symbols:
        prev_rec = prev_map.get(sym) if isinstance(prev_map, dict) else None
        prev_fac = prev_factors_map.get(sym) if isinstance(prev_factors_map, dict) else None
        if not isinstance(prev_rec, dict) or not isinstance(prev_fac, dict):
            continue
        src0 = prev_rec.get("src") if isinstance(prev_rec.get("src"), dict) else {}
        pm = int(src0.get("merge_mtime_ns") or -1)
        pd0 = int(src0.get("dc_mtime_ns") or -1)
        cur_m = _stat_ns(merge_dir / f"{sym}.csv") if (merge_dir / f"{sym}.csv").exists() else -1
        cur_d = _stat_ns(dc_dir / f"{sym}.csv") if (dc_dir / f"{sym}.csv").exists() else -1
        if pm == cur_m and pd0 == cur_d and isinstance(prev_rec.get("dt"), list) and isinstance(prev_rec.get("series"), dict):
            out_series[sym] = prev_rec
            out_factors[sym] = prev_fac
            ok += 1
            reused += 1

    def _to_ts(x) -> Any:
        try:
            return pd.to_datetime(x, utc=True, errors="coerce")
        except Exception:
            return None

    def _ts_ok(x) -> bool:
        try:
            return x is not None and bool(pd.notna(x))
        except Exception:
            return False

    def _max_tail_dt(path0: Path | None, *, tail_hint: int) -> Any:
        if path0 is None or (not path0.exists()):
            return None
        try:
            from apps.crypto_screener.app.series_source import _probe_csv_tail_last_dt
        except Exception:
            return None
        try:
            dt0, n0 = _probe_csv_tail_last_dt(path0, tail_hint=int(tail_hint))
        except Exception:
            return None
        if n0 <= 0 or dt0 is None:
            return None
        return _to_ts(dt0)

    for sym in symbols:
        if sym in out_series:
            continue
        prev_rec = prev_map.get(sym) if isinstance(prev_map, dict) else None
        prev_fac = prev_factors_map.get(sym) if isinstance(prev_factors_map, dict) else None
        if not isinstance(prev_rec, dict) or not isinstance(prev_fac, dict):
            continue
        dt0 = prev_rec.get("dt")
        if not isinstance(dt0, list) or not dt0:
            continue
        prev_last = _to_ts(str(dt0[-1]))
        if not _ts_ok(prev_last):
            continue
        merge_path = _pick_csv_path([merge_dir], sym)
        dc_path = _pick_csv_path([dc_dir], sym)
        dt_m = _max_tail_dt(merge_path, tail_hint=tail)
        dt_d = _max_tail_dt(dc_path, tail_hint=tail)
        best = dt_m
        try:
            if _ts_ok(dt_d) and (best is None or dt_d > best):
                best = dt_d
        except Exception:
            pass
        try:
            if _ts_ok(best) and best <= prev_last:
                out_series[sym] = prev_rec
                out_factors[sym] = prev_fac
                ok += 1
                reused += 1
                reused_by_dt += 1
        except Exception:
            continue
    exec_mode = str(os.environ.get("QC_PKL_CACHE_EXECUTOR") or "").strip().lower()
    use_process = (exec_mode.startswith("p") or exec_mode.startswith("proc")) if exec_mode else (os.name != "nt")
    ex_cls = ProcessPoolExecutor if use_process else ThreadPoolExecutor
    with ex_cls(max_workers=max(1, int(workers))) as ex:
        futs = []
        for sym in symbols:
            if sym in out_series:
                continue
            futs.append(
                ex.submit(
                    _load_one_symbol,
                    market=str(market).lower(),
                    symbol=sym,
                    merge_dir=merge_dir,
                    dc_dir=dc_dir,
                    tail=int(tail),
                    update_tail=int(update_tail),
                    prev=prev_map.get(sym) if isinstance(prev_map, dict) else None,
                )
            )
        for fut in as_completed(futs):
            try:
                sym, srec, frec = fut.result()
            except Exception:
                continue
            if not srec or not isinstance(srec, dict) or not isinstance(srec.get("dt"), list):
                continue
            out_series[sym] = srec
            out_factors[sym] = frec if isinstance(frec, dict) else {}
            ok += 1

    meta = {
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "market": str(market).lower(),
        "tail": int(tail),
        "symbols_found": int(len(symbols)),
        "symbols_ok": int(ok),
        "symbols_reused": int(reused),
        "symbols_reused_by_dt": int(reused_by_dt),
        "merge_dir": str(merge_dir),
        "data_center_dir": str(dc_dir),
        "duration_s": round(time.time() - started, 6),
    }
    try:
        max_dt = None
        for rec in out_series.values():
            if not isinstance(rec, dict):
                continue
            dt0 = rec.get("dt")
            if not isinstance(dt0, list) or not dt0:
                continue
            last0 = dt0[-1]
            t0 = pd.to_datetime(last0, utc=True, errors="coerce")
            if getattr(t0, "tzinfo", None) is None:
                t0 = pd.to_datetime(last0, utc=True, errors="coerce")
            if t0 is None or bool(getattr(t0, "isna", lambda: True)()):
                continue
            if max_dt is None or t0 > max_dt:
                max_dt = t0
        if max_dt is not None:
            meta["max_dt"] = max_dt.isoformat()
    except Exception:
        pass
    series_payload = {"meta": meta, "tail": int(tail), "symbols": out_series}
    factors_payload = {"meta": meta, "tail": int(tail), "factors": out_factors}
    _atomic_pickle_dump(series_path, series_payload)
    _atomic_pickle_dump(factors_path, factors_payload)
    return {"ok": True, "meta": meta, "out_root": str(out_root), "series_path": str(series_path), "factors_path": str(factors_path)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--market", type=str, default="all", choices=["all", "swap", "spot"])
    parser.add_argument("--tail", type=int, default=int(os.environ.get("QC_PKL_CACHE_TAIL") or "2160"))
    parser.add_argument("--symbols", type=int, default=int(os.environ.get("QC_PKL_CACHE_SYMBOLS_LIMIT") or "0"))
    parser.add_argument("--workers", type=int, default=int(os.environ.get("QC_PKL_CACHE_WORKERS") or "8"))
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    m = str(args.market).lower()
    markets = ["swap", "spot"] if m == "all" else [m]
    out = {"ok": True, "results": []}
    for mk in markets:
        try:
            res = build_market_cache(market=mk, tail=int(args.tail), symbols_limit=int(args.symbols), workers=int(args.workers), incremental=(not bool(args.full)))
            out["results"].append(res)
        except Exception as e:
            out["ok"] = False
            out["results"].append({"ok": False, "market": mk, "error": str(e)})
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
