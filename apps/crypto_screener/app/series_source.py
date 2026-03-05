from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timezone
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd


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
    return out


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


def load_symbol_series(*, market: str, symbol: str, tail: int, repo_root: Path | None = None) -> SymbolSeries | None:
    repo_root = repo_root or _repo_root()
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
    dt_list = []
    for x in df["candle_begin_time"].tolist():
        try:
            dt_list.append(x.astimezone(timezone.utc).isoformat())
        except Exception:
            dt_list.append(pd.to_datetime(x, utc=True, errors="coerce").to_pydatetime().replace(tzinfo=timezone.utc).isoformat())
    return SymbolSeries(market=str(market).lower(), symbol=picked_sym or sym, dt=dt_list, series=out_series)
