from __future__ import annotations

import os
import json
from dataclasses import dataclass
from datetime import timezone
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd


_manifest_cache_lock = None
_manifest_cache: dict[str, Any] = {"root": "", "mtime_ns": -1, "index": {}}
_pkl_cache_lock = None
_pkl_cache: dict[str, Any] = {"root": "", "files": {}}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _decode_bytes(raw: bytes) -> str:
    for enc in ("gbk", "utf-8", "latin-1", "cp1252"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


def _default_merge_dirs(repo_root: Path) -> tuple[Path, Path]:
    if os.name == "nt":
        swap0 = Path(os.environ.get("QC_MERGE_SWAP_PATH") or os.environ.get("QC_SCREENER_FALLBACK_SWAP_DIR") or r"D:\量化交易\数据\swap_lin")
        spot0 = Path(os.environ.get("QC_MERGE_SPOT_PATH") or os.environ.get("QC_SCREENER_FALLBACK_SPOT_DIR") or r"D:\量化交易\数据\spot_lin")
        return swap0, spot0
    swap0 = Path(os.environ.get("QC_MERGE_SWAP_PATH") or os.environ.get("QC_SCREENER_FALLBACK_SWAP_DIR") or str(repo_root / "数据获取" / "data" / "swap_lin"))
    spot0 = Path(os.environ.get("QC_MERGE_SPOT_PATH") or os.environ.get("QC_SCREENER_FALLBACK_SPOT_DIR") or str(repo_root / "数据获取" / "data" / "spot_lin"))
    return swap0, spot0


def _default_data_center_dirs(repo_root: Path) -> tuple[Path, Path]:
    root = Path(os.environ.get("QC_DATA_CENTER_ROOT") or str(repo_root / "数据获取" / "data" / "data_center"))
    return root / "kline" / "swap", root / "kline" / "spot"


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


def _preprocessed_enabled() -> bool:
    return str(os.environ.get("QC_USE_PREPROCESSED_SERIES", "1")).strip() != "0"


def _pkl_series_cache_enabled() -> bool:
    return str(os.environ.get("QC_USE_PKL_SERIES_CACHE", "1")).strip() != "0"


def _pkl_series_cache_root(repo_root: Path) -> Path:
    env = (os.environ.get("QC_PKL_CACHE_ROOT") or "").strip()
    if env:
        return Path(env)
    return _preprocessed_root(repo_root) / "pkl_cache"


def _pkl_lock():
    global _pkl_cache_lock
    if _pkl_cache_lock is None:
        import threading

        _pkl_cache_lock = threading.Lock()
    return _pkl_cache_lock


def _load_pkl_series_cache(root: Path, *, market: str) -> dict[str, Any]:
    fp = root / f"series_{str(market).lower()}.pkl"
    if not fp.exists():
        return {}
    try:
        st_mtime_ns = int(fp.stat().st_mtime_ns)
    except Exception:
        st_mtime_ns = -1
    lock = _pkl_lock()
    with lock:
        if _pkl_cache.get("root") == str(root):
            files0 = _pkl_cache.get("files")
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
        if _pkl_cache.get("root") != str(root):
            _pkl_cache["root"] = str(root)
            _pkl_cache["files"] = {}
        files1 = _pkl_cache.get("files")
        if not isinstance(files1, dict):
            files1 = {}
            _pkl_cache["files"] = files1
        files1[str(market).lower()] = {"mtime_ns": st_mtime_ns, "data": payload}
        return payload


def _stat_mtime_ns(p: Path | None) -> int:
    if p is None:
        return -1
    try:
        st = p.stat()
        return int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9)))
    except Exception:
        return -1


def get_series_versions(*, market: str, symbol: str, tail: int, repo_root: Path) -> dict:
    mk = str(market).lower()
    root = _pkl_series_cache_root(repo_root)
    pkl_path = root / f"series_{mk}.pkl"
    pkl_mtime_ns = _stat_mtime_ns(pkl_path) if pkl_path.exists() else -1
    swap_dir, spot_dir = _default_merge_dirs(repo_root)
    dc_swap_dir, dc_spot_dir = _default_data_center_dirs(repo_root)
    is_swap = mk == "swap"
    base = swap_dir if is_swap else spot_dir
    dc_base = dc_swap_dir if is_swap else dc_spot_dir
    csv_path, _picked = _pick_existing_csv([base, dc_base], str(symbol or "").strip(), tail_hint=int(tail))
    csv_mtime_ns = _stat_mtime_ns(csv_path) if csv_path is not None and csv_path.exists() else -1
    return {
        "csv_path": str(csv_path) if csv_path is not None else "",
        "csv_mtime_ns": int(csv_mtime_ns),
        "pkl_path": str(pkl_path),
        "pkl_mtime_ns": int(pkl_mtime_ns),
    }


def _to_utc_ts(x) -> Any:
    try:
        return pd.to_datetime(x, utc=True, errors="coerce")
    except Exception:
        return None


def _load_series_from_pkl_cache(*, market: str, symbol: str, tail: int, repo_root: Path, return_stale: bool = False) -> tuple[SymbolSeries | None, bool]:
    root = _pkl_series_cache_root(repo_root)
    if not root.exists():
        return (None, False) if return_stale else (None, False)
    pkl_path = root / f"series_{str(market).lower()}.pkl"
    if not pkl_path.exists():
        return (None, False) if return_stale else (None, False)
    try:
        pkl_mtime_ns = int(pkl_path.stat().st_mtime_ns)
    except Exception:
        pkl_mtime_ns = -1
    stale = False
    try:
        if str(os.environ.get("QC_PKL_REQUIRE_FRESH") or "1").strip() != "0":
            swap_dir, spot_dir = _default_merge_dirs(repo_root)
            dc_swap_dir, dc_spot_dir = _default_data_center_dirs(repo_root)
            is_swap = str(market).lower() == "swap"
            base = swap_dir if is_swap else spot_dir
            dc_base = dc_swap_dir if is_swap else dc_spot_dir
            csv_path, _picked = _pick_existing_csv([base, dc_base], str(symbol or "").strip(), tail_hint=tail)
            if csv_path is not None:
                try:
                    csv_mtime_ns = int(csv_path.stat().st_mtime_ns)
                except Exception:
                    csv_mtime_ns = -1
                if csv_mtime_ns > 0 and pkl_mtime_ns > 0 and csv_mtime_ns > pkl_mtime_ns:
                    stale = True
                if not stale:
                    dt_csv, n_csv = _probe_csv_tail_last_dt(csv_path, tail_hint=tail)
                    if n_csv > 0 and dt_csv is not None:
                        payload0 = _load_pkl_series_cache(root, market=str(market).lower())
                        syms0 = payload0.get("symbols") if isinstance(payload0, dict) else None
                        if isinstance(syms0, dict):
                            best0 = None
                            for s in _symbol_candidates(symbol):
                                rec0 = syms0.get(str(s).upper())
                                if isinstance(rec0, dict) and isinstance(rec0.get("dt"), list) and rec0.get("dt"):
                                    best0 = rec0
                                    break
                            if best0 is not None:
                                dt_pkl_last = best0.get("dt")[-1]
                                a = _to_utc_ts(dt_pkl_last)
                                b = _to_utc_ts(dt_csv)
                                try:
                                    if a is not None and b is not None and bool(b.notna()) and bool(a.notna()) and b > a:
                                        stale = True
                                except Exception:
                                    pass
    except Exception:
        stale = False
    if stale:
        return (None, True) if return_stale else (None, False)

    payload = _load_pkl_series_cache(root, market=str(market).lower())
    syms = payload.get("symbols") if isinstance(payload, dict) else None
    if not isinstance(syms, dict):
        return (None, False) if return_stale else (None, False)
    cands = _symbol_candidates(symbol)
    best_sym = ""
    best = None
    for s in cands:
        rec = syms.get(str(s).upper())
        if isinstance(rec, dict) and isinstance(rec.get("dt"), list) and isinstance(rec.get("series"), dict):
            best = rec
            best_sym = str(s).upper()
            break
    if best is None:
        return (None, False) if return_stale else (None, False)
    dt0 = best.get("dt")
    series0 = best.get("series")
    if not isinstance(dt0, list) or not isinstance(series0, dict) or not dt0:
        return (None, False) if return_stale else (None, False)
    need = max(80, min(3650, int(tail)))
    dt = dt0[-need:] if len(dt0) > need else list(dt0)
    out_series: dict[str, list[float | None]] = {}
    for col in ("open", "high", "low", "close", "volume", "quote_volume"):
        arr = series0.get(col)
        if isinstance(arr, list) and arr:
            xs = arr[-len(dt) :] if len(arr) >= len(dt) else list(arr)
            if len(xs) != len(dt):
                n = min(len(xs), len(dt))
                xs = xs[-n:]
                dt = dt[-n:]
            out_series[col] = xs
    if not dt:
        return (None, False) if return_stale else (None, False)
    s_out = SymbolSeries(market=str(market).lower(), symbol=best_sym or str(symbol).upper(), dt=dt, series=out_series, source="pkl_cache")
    return (s_out, False) if return_stale else (s_out, False)


def _preprocessed_root(repo_root: Path) -> Path:
    env = (os.environ.get("QC_PREPROCESS_OUT_ROOT") or "").strip()
    if env:
        return Path(env)
    return repo_root / "数据获取" / "data" / "preprocessed_hourly"


def _manifest_lock():
    global _manifest_cache_lock
    if _manifest_cache_lock is None:
        import threading

        _manifest_cache_lock = threading.Lock()
    return _manifest_cache_lock


def _read_preprocessed_file(path: Path, *, columns: list[str]) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext == ".pkl":
        df = pd.read_pickle(path)
        keep = [c for c in columns if c in df.columns]
        return df[keep].copy() if keep else pd.DataFrame()
    return pd.DataFrame()


def _load_manifest_index(root: Path) -> dict[tuple[str, str], list[tuple[str, Path]]]:
    mf = root / "manifest.json"
    if not mf.exists():
        return {}
    try:
        st_mtime_ns = int(mf.stat().st_mtime_ns)
    except Exception:
        st_mtime_ns = -1
    lock = _manifest_lock()
    with lock:
        if _manifest_cache.get("root") == str(root) and int(_manifest_cache.get("mtime_ns", -1)) == st_mtime_ns:
            idx0 = _manifest_cache.get("index")
            return idx0 if isinstance(idx0, dict) else {}
        try:
            payload = json.loads(mf.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        index: dict[tuple[str, str], list[tuple[str, Path]]] = {}
        latest_end: dict[tuple[str, str], str] = {}
        files = payload.get("files") if isinstance(payload, dict) else []
        if isinstance(files, list):
            for rec in files:
                if not isinstance(rec, dict):
                    continue
                market = str(rec.get("market") or "").strip().lower()
                symbol = str(rec.get("symbol") or "").strip().upper()
                part = str(rec.get("partition") or "").strip()
                fn = str(rec.get("file") or "").strip()
                if not market or not symbol or not part or not fn:
                    continue
                p = root / part / fn
                if not p.exists():
                    continue
                key = (market, symbol)
                arr = index.get(key)
                if arr is None:
                    arr = []
                    index[key] = arr
                arr.append((part, p))
                tr = rec.get("time_range")
                if isinstance(tr, dict):
                    end0 = tr.get("end")
                    if end0 is not None:
                        try:
                            prev = latest_end.get(key)
                            if prev is None or str(end0) > str(prev):
                                latest_end[key] = str(end0)
                        except Exception:
                            pass
        for k, arr in index.items():
            arr.sort(key=lambda x: x[0])
        _manifest_cache["root"] = str(root)
        _manifest_cache["mtime_ns"] = st_mtime_ns
        _manifest_cache["index"] = index
        _manifest_cache["latest_end"] = latest_end
        return index


def _preprocessed_latest_end_dt(*, market: str, symbol: str, repo_root: Path) -> Any:
    root = _preprocessed_root(repo_root)
    if not root.exists():
        return None
    _load_manifest_index(root)
    lock = _manifest_lock()
    with lock:
        mp = _manifest_cache.get("latest_end")
        if not isinstance(mp, dict):
            return None
        best = None
        for s in _symbol_candidates(symbol):
            v = mp.get((str(market).lower(), str(s).upper()))
            if not v:
                continue
            if best is None or str(v) > str(best):
                best = v
    if best is None:
        return None
    try:
        return pd.to_datetime(best, utc=True, errors="coerce")
    except Exception:
        return None


def _load_series_from_preprocessed(*, market: str, symbol: str, tail: int, repo_root: Path) -> SymbolSeries | None:
    root = _preprocessed_root(repo_root)
    if not root.exists():
        return None
    index = _load_manifest_index(root)
    cands = _symbol_candidates(symbol)
    best_sym = ""
    best_files: list[tuple[str, Path]] = []
    for s in cands:
        fs = index.get((str(market).lower(), str(s).upper())) or []
        if len(fs) > len(best_files):
            best_files = fs
            best_sym = str(s).upper()
    if not best_files:
        return None
    need = max(80, min(3650, int(tail)))
    pick = best_files[-max(need * 2, need + 32) :]
    cols = ["dt", "open", "high", "low", "close", "volume", "quote_volume"]
    blocks: list[pd.DataFrame] = []
    for _, path in pick:
        try:
            df = _read_preprocessed_file(path, columns=cols)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        blocks.append(df)
    if not blocks:
        return None
    merged = pd.concat(blocks, ignore_index=True)
    if "dt" not in merged.columns:
        return None
    merged["dt"] = pd.to_datetime(merged["dt"], errors="coerce", utc=True)
    merged = merged.dropna(subset=["dt"]).sort_values("dt")
    merged = merged.drop_duplicates(subset=["dt"], keep="last").tail(need).copy()
    if merged.empty:
        return None
    out_series: dict[str, list[float | None]] = {}
    for col in ("open", "high", "low", "close", "volume", "quote_volume"):
        if col in merged.columns:
            vals = pd.to_numeric(merged[col], errors="coerce").tolist()
            out_series[col] = [None if (v is None or (isinstance(v, float) and pd.isna(v))) else float(v) for v in vals]
    dt_list: list[str] = []
    for x in merged["dt"].tolist():
        try:
            dt_list.append(x.astimezone(timezone.utc).isoformat())
        except Exception:
            dt_list.append(pd.to_datetime(x, utc=True, errors="coerce").to_pydatetime().replace(tzinfo=timezone.utc).isoformat())
    if not dt_list:
        return None
    return SymbolSeries(market=str(market).lower(), symbol=best_sym or str(symbol).upper(), dt=dt_list, series=out_series, source="preprocessed")


def _probe_csv_tail_last_dt(path: Path, *, tail_hint: int) -> tuple[Any, int]:
    try:
        t = int(tail_hint)
    except Exception:
        t = 360
    t = max(30, min(3650, t))
    try:
        df = read_merge_csv_tail(path, tail=min(120, t), extra=80)
    except Exception:
        return None, 0
    if df is None or df.empty or "candle_begin_time" not in df.columns:
        return None, 0
    try:
        dt = df["candle_begin_time"].iloc[-1]
    except Exception:
        dt = None
    try:
        n = int(len(df))
    except Exception:
        n = 0
    return dt, n


def _pick_existing_csv(base_dirs: list[Path], symbol: str, *, tail_hint: int) -> tuple[Path | None, str]:
    cands = _symbol_candidates(symbol)
    found: list[tuple[int, Path, str]] = []
    for bi, base in enumerate(list(base_dirs or [])):
        base_r = base.resolve()
        for sym in cands:
            p = (base / f"{sym}.csv").resolve()
            if base_r not in p.parents and p != base_r:
                continue
            if p.exists() and p.is_file():
                found.append((bi, p, sym))
    if not found:
        return None, ""
    try:
        t = int(tail_hint)
    except Exception:
        t = 360
    min_rows = max(30, min(120, max(0, t) // 6))
    best = None
    best_score = None
    for bi, p, sym in found:
        dt0, n0 = _probe_csv_tail_last_dt(p, tail_hint=t)
        dt_ms = -1
        try:
            dt_ms = int(getattr(dt0, "value", 0) // 1_000_000)
        except Exception:
            dt_ms = -1
        ok_rows = int(n0) if int(n0) >= min_rows else 0
        try:
            size = int(p.stat().st_size)
        except Exception:
            size = 0
        score = (dt_ms, ok_rows, size, -int(bi))
        if best_score is None or score > best_score:
            best_score = score
            best = (p, sym)
    return best if best is not None else (found[0][1], found[0][2])


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
        if "candle_begin_time" in lines[0]:
            return lines[0]
        if len(lines) >= 2 and "candle_begin_time" in lines[1]:
            return lines[1]
        for ln in lines[:10]:
            if "candle_begin_time" in ln:
                return ln
        return lines[0]
    except Exception:
        return ""


def read_merge_csv_tail(path: Path, *, tail: int, extra: int = 300) -> pd.DataFrame:
    header = _read_header_line(path)
    lines = _read_tail_text(path, max_lines=max(2000, int(tail) + int(extra)))
    if not header or not lines:
        return pd.DataFrame()
    header_clean = header.strip().lstrip("\ufeff").strip()
    body = []
    for ln in lines:
        ln0 = ln.strip()
        if not ln0:
            continue
        if ln0.lstrip("\ufeff").strip() == header_clean:
            continue
        if "candle_begin_time" in ln0:
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
    if df.empty or "candle_begin_time" not in df.columns:
        return pd.DataFrame()
    s = df["candle_begin_time"]
    dt = pd.to_datetime(s, errors="coerce", utc=True, format="mixed")
    if dt.notna().sum() < max(3, int(len(dt) * 0.2)):
        xs = pd.to_numeric(s, errors="coerce")
        if xs.notna().sum() >= max(3, int(len(xs) * 0.8)):
            dt_ms = pd.to_datetime(xs, errors="coerce", utc=True, unit="ms")
            dt_s = pd.to_datetime(xs, errors="coerce", utc=True, unit="s")
            if dt_ms.notna().sum() >= dt.notna().sum():
                dt = dt_ms
            if dt_s.notna().sum() > dt.notna().sum():
                dt = dt_s
    df["candle_begin_time"] = dt
    df = df.dropna(subset=["candle_begin_time"]).sort_values("candle_begin_time")
    return df.tail(max(1, int(tail))).copy()


@dataclass(frozen=True)
class SymbolSeries:
    market: str
    symbol: str
    dt: list[str]
    series: dict[str, list[float | None]]
    source: str = "csv"


def load_symbol_series(*, market: str, symbol: str, tail: int, repo_root: Path | None = None) -> SymbolSeries | None:
    repo_root = repo_root or _repo_root()
    pkl_stale = False
    if _pkl_series_cache_enabled():
        try:
            s_cache, pkl_stale = _load_series_from_pkl_cache(market=market, symbol=symbol, tail=tail, repo_root=repo_root, return_stale=True)
        except Exception:
            s_cache = None
            pkl_stale = False
        if s_cache is not None:
            return s_cache
    if pkl_stale:
        try:
            swap_dir, spot_dir = _default_merge_dirs(repo_root)
            dc_swap_dir, dc_spot_dir = _default_data_center_dirs(repo_root)
            is_swap = str(market).lower() == "swap"
            base = swap_dir if is_swap else spot_dir
            dc_base = dc_swap_dir if is_swap else dc_spot_dir
            sym = str(symbol or "").strip()
            csv_path, picked_sym = _pick_existing_csv([base, dc_base], sym, tail_hint=tail)
            if csv_path is not None:
                df = read_merge_csv_tail(csv_path, tail=tail)
                if not df.empty:
                    out_series: dict[str, list[float | None]] = {}
                    for col in ("open", "high", "low", "close", "volume", "quote_volume"):
                        if col in df.columns:
                            vals = pd.to_numeric(df[col], errors="coerce").tolist()
                            out_series[col] = [None if (v is None or (isinstance(v, float) and pd.isna(v))) else float(v) for v in vals]
                        elif col == "quote_volume" and "quotevolume" in df.columns:
                            vals = pd.to_numeric(df["quotevolume"], errors="coerce").tolist()
                            out_series[col] = [None if (v is None or (isinstance(v, float) and pd.isna(v))) else float(v) for v in vals]
                    dt_list = []
                    for x in df["candle_begin_time"].tolist():
                        try:
                            dt_list.append(x.astimezone(timezone.utc).isoformat())
                        except Exception:
                            dt_list.append(pd.to_datetime(x, utc=True, errors="coerce").to_pydatetime().replace(tzinfo=timezone.utc).isoformat())
                    return SymbolSeries(market=str(market).lower(), symbol=picked_sym or sym, dt=dt_list, series=out_series, source="csv")
        except Exception:
            pass

    if _preprocessed_enabled():
        try:
            if str(os.environ.get("QC_PREPROCESS_REQUIRE_FRESH") or "1").strip() != "0":
                swap_dir, spot_dir = _default_merge_dirs(repo_root)
                dc_swap_dir, dc_spot_dir = _default_data_center_dirs(repo_root)
                is_swap = str(market).lower() == "swap"
                base = swap_dir if is_swap else spot_dir
                dc_base = dc_swap_dir if is_swap else dc_spot_dir
                csv_path0, _picked0 = _pick_existing_csv([base, dc_base], str(symbol or "").strip(), tail_hint=tail)
                if csv_path0 is not None:
                    dt_csv, n_csv = _probe_csv_tail_last_dt(csv_path0, tail_hint=tail)
                    if n_csv > 0 and dt_csv is not None:
                        b = _to_utc_ts(dt_csv)
                        a = _preprocessed_latest_end_dt(market=market, symbol=symbol, repo_root=repo_root)
                        try:
                            if a is not None and b is not None and bool(b.notna()) and bool(a.notna()) and b > a:
                                swap_dir, spot_dir = _default_merge_dirs(repo_root)
                                dc_swap_dir, dc_spot_dir = _default_data_center_dirs(repo_root)
                                is_swap = str(market).lower() == "swap"
                                base = swap_dir if is_swap else spot_dir
                                dc_base = dc_swap_dir if is_swap else dc_spot_dir
                                sym = str(symbol or "").strip()
                                csv_path, picked_sym = _pick_existing_csv([base, dc_base], sym, tail_hint=tail)
                                if csv_path is not None:
                                    df = read_merge_csv_tail(csv_path, tail=tail)
                                    if not df.empty:
                                        out_series: dict[str, list[float | None]] = {}
                                        for col in ("open", "high", "low", "close", "volume", "quote_volume"):
                                            if col in df.columns:
                                                vals = pd.to_numeric(df[col], errors="coerce").tolist()
                                                out_series[col] = [None if (v is None or (isinstance(v, float) and pd.isna(v))) else float(v) for v in vals]
                                            elif col == "quote_volume" and "quotevolume" in df.columns:
                                                vals = pd.to_numeric(df["quotevolume"], errors="coerce").tolist()
                                                out_series[col] = [None if (v is None or (isinstance(v, float) and pd.isna(v))) else float(v) for v in vals]
                                        dt_list = []
                                        for x in df["candle_begin_time"].tolist():
                                            try:
                                                dt_list.append(x.astimezone(timezone.utc).isoformat())
                                            except Exception:
                                                dt_list.append(pd.to_datetime(x, utc=True, errors="coerce").to_pydatetime().replace(tzinfo=timezone.utc).isoformat())
                                        return SymbolSeries(market=str(market).lower(), symbol=picked_sym or sym, dt=dt_list, series=out_series, source="csv")
                        except Exception:
                            pass
        except Exception:
            pass
        try:
            s_pre = _load_series_from_preprocessed(market=market, symbol=symbol, tail=tail, repo_root=repo_root)
        except Exception:
            s_pre = None
        if s_pre is not None:
            return s_pre
    swap_dir, spot_dir = _default_merge_dirs(repo_root)
    dc_swap_dir, dc_spot_dir = _default_data_center_dirs(repo_root)
    is_swap = str(market).lower() == "swap"
    base = swap_dir if is_swap else spot_dir
    dc_base = dc_swap_dir if is_swap else dc_spot_dir
    sym = str(symbol or "").strip()
    csv_path, picked_sym = _pick_existing_csv([base, dc_base], sym, tail_hint=tail)
    if csv_path is None:
        return None
    base_r = csv_path.parent.resolve()
    if base_r not in csv_path.parents and csv_path != base_r:
        return None
    df = read_merge_csv_tail(csv_path, tail=tail)
    if df.empty:
        return None

    out_series: dict[str, list[float | None]] = {}
    for col in ("open", "high", "low", "close", "volume", "quote_volume"):
        if col in df.columns:
            vals = pd.to_numeric(df[col], errors="coerce").tolist()
            out_series[col] = [None if (v is None or (isinstance(v, float) and pd.isna(v))) else float(v) for v in vals]
        elif col == "quote_volume" and "quotevolume" in df.columns:
            vals = pd.to_numeric(df["quotevolume"], errors="coerce").tolist()
            out_series[col] = [None if (v is None or (isinstance(v, float) and pd.isna(v))) else float(v) for v in vals]
    dt_list = []
    for x in df["candle_begin_time"].tolist():
        try:
            dt_list.append(x.astimezone(timezone.utc).isoformat())
        except Exception:
            dt_list.append(pd.to_datetime(x, utc=True, errors="coerce").to_pydatetime().replace(tzinfo=timezone.utc).isoformat())
    return SymbolSeries(market=str(market).lower(), symbol=picked_sym or sym, dt=dt_list, series=out_series, source="csv")
