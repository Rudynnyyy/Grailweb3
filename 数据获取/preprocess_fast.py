from __future__ import annotations

import hashlib
import json
import math
import os
import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


CSV_COLS = (
    "candle_begin_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_num",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
)


CANONICAL_COLS = (
    "dt",
    "symbol",
    "market",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_num",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ohlc4",
    "ret1",
    "logret1",
    "ma_5",
    "ma_10",
    "ma_20",
    "ma_30",
    "ma_60",
    "ma_120",
    "ema_5",
    "ema_10",
    "ema_20",
    "ema_30",
    "ema_60",
    "ema_120",
    "rsi_6",
    "rsi_14",
    "rsi_21",
    "rolling_std_20",
)


@dataclass(frozen=True)
class PreprocessConfig:
    source_swap_dir: Path
    source_spot_dir: Path
    output_root: Path
    file_format: str = "parquet"
    compression: str = "zstd"
    market_order: tuple[str, ...] = ("swap", "spot")
    include_optional_indicators: bool = True


def _to_utc_hour(ts: pd.Timestamp) -> pd.Timestamp:
    return pd.to_datetime(ts, utc=True, errors="coerce").floor("h")


def _safe_symbol(s: str) -> str:
    return str(s or "").strip().upper()


def _symbol_candidates(symbol: str) -> list[str]:
    s = _safe_symbol(symbol)
    if not s:
        return []
    out: list[str] = [s]
    if s.endswith("-USDT"):
        out.append(s.replace("-", ""))
    elif s.endswith("USDT"):
        out.append(f"{s[:-4]}-USDT")
    seen: set[str] = set()
    dedup: list[str] = []
    for x in out:
        if x not in seen:
            seen.add(x)
            dedup.append(x)
    return dedup


def find_source_csv(source_dir: Path, symbol: str) -> Path | None:
    if not source_dir.exists():
        return None
    for sym in _symbol_candidates(symbol):
        p = source_dir / f"{sym}.csv"
        if p.exists():
            return p
    return None


def _decode_bytes(raw: bytes) -> str:
    for enc in ("gbk", "utf-8", "latin-1", "cp1252"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


def _read_header_line(path: Path) -> str:
    with path.open("rb") as f:
        raw = f.read(65536)
    text = _decode_bytes(raw)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return ""
    if "candle_begin_time" in lines[0]:
        return lines[0]
    for ln in lines[1:8]:
        if "candle_begin_time" in ln:
            return ln
    return lines[0]


def read_csv_tail(path: Path, tail: int = 5000, extra: int = 400) -> pd.DataFrame:
    header = _read_header_line(path)
    if not header:
        return pd.DataFrame()
    with path.open("rb") as f:
        f.seek(0, os.SEEK_END)
        end = f.tell()
        block = 65536
        buf = b""
        pos = end
        need = max(300, int(tail) + int(extra) + 5)
        while pos > 0 and buf.count(b"\n") < need:
            step = block if pos >= block else pos
            pos -= step
            f.seek(pos)
            buf = f.read(step) + buf
    text = _decode_bytes(buf)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    clean_header = header.lstrip("\ufeff").strip()
    body = []
    for ln in lines:
        x = ln.lstrip("\ufeff").strip()
        if not x:
            continue
        if x == clean_header:
            continue
        if "candle_begin_time" in x:
            continue
        body.append(x)
    from io import StringIO

    csv_text = f"{header}\n" + "\n".join(body[-(tail + extra) :])
    try:
        df = pd.read_csv(StringIO(csv_text), sep=None, engine="python", on_bad_lines="skip")
    except TypeError:
        df = pd.read_csv(StringIO(csv_text), sep=None, engine="python")
    if df.empty:
        return df
    df.columns = [str(c).lstrip("\ufeff").strip() for c in list(df.columns)]
    return df.tail(tail).copy()


def read_csv_all(path: Path) -> pd.DataFrame:
    for enc in ("gbk", "utf-8", "latin-1", "cp1252"):
        for skip in (0, 1):
            try:
                df = pd.read_csv(path, encoding=enc, sep=None, engine="python", on_bad_lines="skip", skiprows=skip)
            except Exception:
                continue
            if df is None or df.empty:
                continue
            cols = [str(c).strip() for c in list(df.columns)]
            if "candle_begin_time" in cols:
                return df
            if len(cols) >= 1 and "数据由币安API获取" in cols[0]:
                continue
            if skip == 0:
                continue
            return df
    return pd.DataFrame()


def _coerce_dt(raw: pd.Series) -> pd.Series:
    xs = pd.to_numeric(raw, errors="coerce")
    n = len(raw)
    num_cnt = int(xs.notna().sum())
    if n > 0 and num_cnt >= max(3, int(n * 0.7)):
        med = float(xs.dropna().median())
        unit = "ms" if med > 10_000_000_000 else "s"
        return pd.to_datetime(xs, errors="coerce", utc=True, unit=unit)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        return pd.to_datetime(raw, errors="coerce", utc=True)


def _map_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    cols = [str(c).strip() for c in list(out.columns)]
    out.columns = cols
    rename_map = {
        "candle_begin_time": "dt",
        "quotevolume": "quote_volume",
        "fundingRate": "funding_rate",
    }
    out = out.rename(columns=rename_map)
    if "dt" not in out.columns and len(out.columns) > 0:
        out = out.rename(columns={out.columns[0]: "dt"})
    missing_base = [x for x in CSV_COLS if x != "candle_begin_time" and x not in out.columns]
    for c in missing_base:
        out[c] = np.nan
    return out


def _rsi_vectorized(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / float(period), adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / float(period), adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.where(avg_loss != 0, 100)
    rsi = rsi.where(avg_gain != 0, 0)
    return rsi


def preprocess_frame(df: pd.DataFrame, *, market: str, symbol: str, include_optional_indicators: bool = True) -> pd.DataFrame:
    mapped = _map_columns(df)
    mapped["dt"] = _coerce_dt(mapped["dt"])
    mapped = mapped.dropna(subset=["dt"])
    for col in ("open", "high", "low", "close", "volume", "quote_volume", "trade_num", "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume"):
        mapped[col] = pd.to_numeric(mapped[col], errors="coerce")
    mapped = mapped.sort_values("dt")
    mapped = mapped.drop_duplicates(subset=["dt"], keep="last")
    mapped["symbol"] = _safe_symbol(symbol)
    mapped["market"] = str(market).lower()
    mapped["ohlc4"] = (mapped["open"] + mapped["high"] + mapped["low"] + mapped["close"]) / 4.0
    mapped["ret1"] = mapped["close"].pct_change()
    mapped["logret1"] = np.log(mapped["close"].replace(0, np.nan)).diff()
    if include_optional_indicators:
        for w in (5, 10, 20, 30, 60, 120):
            mapped[f"ma_{w}"] = mapped["close"].rolling(window=w, min_periods=w).mean()
            mapped[f"ema_{w}"] = mapped["close"].ewm(span=w, adjust=False, min_periods=w).mean()
        for p in (6, 14, 21):
            mapped[f"rsi_{p}"] = _rsi_vectorized(mapped["close"], p)
        mapped["rolling_std_20"] = mapped["close"].rolling(window=20, min_periods=20).std(ddof=0)
    for col in CANONICAL_COLS:
        if col not in mapped.columns:
            mapped[col] = np.nan
    mapped = mapped[list(CANONICAL_COLS)].copy()
    mapped["dt"] = pd.to_datetime(mapped["dt"], utc=True, errors="coerce")
    return mapped


def _partition_dir(root: Path, market: str, dt: pd.Timestamp) -> Path:
    t = _to_utc_hour(dt)
    return root / str(market).lower() / f"{t.year:04d}" / f"{t.month:02d}" / f"{t.day:02d}" / f"{t.hour:02d}"


def _file_hash(path: Path, chunk: int = 1024 * 1024) -> str:
    md5 = hashlib.md5()
    with path.open("rb") as f:
        while True:
            buf = f.read(chunk)
            if not buf:
                break
            md5.update(buf)
    return md5.hexdigest()


def _write_df(df: pd.DataFrame, path: Path, file_format: str, compression: str) -> str:
    fmt = str(file_format).lower()
    if fmt == "parquet":
        try:
            df.to_parquet(path, index=False, compression=compression)
            return "parquet"
        except Exception:
            df.to_pickle(path.with_suffix(".pkl"), protocol=5)
            return "pkl"
    df.to_pickle(path.with_suffix(".pkl"), protocol=5)
    return "pkl"


def write_hour_partition(
    *,
    root: Path,
    market: str,
    symbol: str,
    dt_hour: pd.Timestamp,
    frame: pd.DataFrame,
    file_format: str,
    compression: str,
) -> dict[str, Any]:
    pdir = _partition_dir(root, market, dt_hour)
    pdir.mkdir(parents=True, exist_ok=True)
    stem = _safe_symbol(symbol)
    target_parquet = pdir / f"{stem}.parquet"
    used = _write_df(frame, target_parquet, file_format, compression)
    out_path = target_parquet if used == "parquet" else pdir / f"{stem}.pkl"
    hash_v = _file_hash(out_path)
    rows = int(len(frame))
    start_dt = pd.to_datetime(frame["dt"].min(), utc=True).isoformat() if rows else None
    end_dt = pd.to_datetime(frame["dt"].max(), utc=True).isoformat() if rows else None
    rec = {
        "market": str(market).lower(),
        "symbol": stem,
        "partition": pdir.relative_to(root).as_posix(),
        "file": out_path.name,
        "format": used,
        "compression": compression if used == "parquet" else "pickle-protocol5",
        "row_count": rows,
        "time_range": {"start": start_dt, "end": end_dt},
        "md5": hash_v,
        "size_bytes": int(out_path.stat().st_size),
        "columns": list(frame.columns),
    }
    manifest_path = pdir / "manifest.json"
    manifest: dict[str, Any] = {"version": 1, "partition": rec["partition"], "files": []}
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            manifest = {"version": 1, "partition": rec["partition"], "files": []}
    files = [x for x in manifest.get("files", []) if not (x.get("symbol") == rec["symbol"] and x.get("market") == rec["market"])]
    files.append(rec)
    files = sorted(files, key=lambda x: (str(x.get("market", "")), str(x.get("symbol", ""))))
    manifest["generated_at"] = datetime.now(timezone.utc).isoformat()
    manifest["files"] = files
    manifest["row_count"] = int(sum(int(x.get("row_count", 0)) for x in files))
    starts = [x.get("time_range", {}).get("start") for x in files if x.get("time_range", {}).get("start")]
    ends = [x.get("time_range", {}).get("end") for x in files if x.get("time_range", {}).get("end")]
    manifest["time_range"] = {"start": min(starts) if starts else None, "end": max(ends) if ends else None}
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    success_path = pdir / "_SUCCESS"
    success_path.write_text(datetime.now(timezone.utc).isoformat(), encoding="utf-8")
    return rec


def build_global_manifest(output_root: Path) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    for market in ("swap", "spot"):
        mroot = output_root / market
        if not mroot.exists():
            continue
        for mf in mroot.rglob("manifest.json"):
            try:
                data = json.loads(mf.read_text(encoding="utf-8"))
            except Exception:
                continue
            for x in data.get("files", []):
                rec = dict(x)
                rec["manifest_file"] = mf.relative_to(output_root).as_posix()
                records.append(rec)
    records = sorted(records, key=lambda x: (str(x.get("partition", "")), str(x.get("symbol", ""))))
    g = {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_partitions": len({str(x.get("partition", "")) for x in records}),
        "total_files": len(records),
        "total_rows": int(sum(int(x.get("row_count", 0)) for x in records)),
        "files": records,
    }
    (output_root / "manifest.json").write_text(json.dumps(g, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    (output_root / "_SUCCESS").write_text(datetime.now(timezone.utc).isoformat(), encoding="utf-8")
    return g


def process_symbol_file(
    *,
    csv_path: Path,
    market: str,
    output_root: Path,
    file_format: str = "parquet",
    compression: str = "zstd",
    include_optional_indicators: bool = True,
    start_hour: pd.Timestamp | None = None,
    end_hour: pd.Timestamp | None = None,
) -> list[dict[str, Any]]:
    symbol = csv_path.stem
    raw = read_csv_all(csv_path)
    if raw.empty:
        return []
    pre = preprocess_frame(raw, market=market, symbol=symbol, include_optional_indicators=include_optional_indicators)
    if pre.empty:
        return []
    pre["hour"] = pre["dt"].dt.floor("h")
    if start_hour is not None:
        st = _to_utc_hour(start_hour)
        pre = pre[pre["hour"] >= st]
    if end_hour is not None:
        et = _to_utc_hour(end_hour)
        pre = pre[pre["hour"] <= et]
    if pre.empty:
        return []
    out: list[dict[str, Any]] = []
    for h, g in pre.groupby("hour", sort=True):
        block = g.drop(columns=["hour"]).copy()
        rec = write_hour_partition(
            root=output_root,
            market=market,
            symbol=symbol,
            dt_hour=pd.to_datetime(h, utc=True),
            frame=block,
            file_format=file_format,
            compression=compression,
        )
        out.append(rec)
    return out


def preprocess_market(
    *,
    market: str,
    source_dir: Path,
    output_root: Path,
    file_format: str,
    compression: str,
    include_optional_indicators: bool,
    start_hour: pd.Timestamp | None = None,
    end_hour: pd.Timestamp | None = None,
) -> list[dict[str, Any]]:
    if not source_dir.exists():
        return []
    recs: list[dict[str, Any]] = []
    errs: list[str] = []
    for csv_path in sorted(source_dir.glob("*.csv")):
        try:
            recs.extend(
                process_symbol_file(
                    csv_path=csv_path,
                    market=market,
                    output_root=output_root,
                    file_format=file_format,
                    compression=compression,
                    include_optional_indicators=include_optional_indicators,
                    start_hour=start_hour,
                    end_hour=end_hour,
                )
            )
        except Exception:
            try:
                errs.append(str(csv_path))
            except Exception:
                errs.append("unknown")
            continue
    if not recs and errs:
        raise RuntimeError(f"preprocess_market_no_output market={market} source_dir={source_dir} error_files={errs[:3]}")
    return recs


def preprocess_all(config: PreprocessConfig, *, start_hour: pd.Timestamp | None = None, end_hour: pd.Timestamp | None = None) -> dict[str, Any]:
    config.output_root.mkdir(parents=True, exist_ok=True)
    all_recs: list[dict[str, Any]] = []
    for market in config.market_order:
        if market == "swap":
            src = config.source_swap_dir
        elif market == "spot":
            src = config.source_spot_dir
        else:
            continue
        all_recs.extend(
            preprocess_market(
                market=market,
                source_dir=src,
                output_root=config.output_root,
                file_format=config.file_format,
                compression=config.compression,
                include_optional_indicators=config.include_optional_indicators,
                start_hour=start_hour,
                end_hour=end_hour,
            )
        )
    global_manifest = build_global_manifest(config.output_root)
    return {"records": all_recs, "global_manifest": global_manifest}


def infer_default_config(repo_root: Path) -> PreprocessConfig:
    data_root = repo_root / "数据获取" / "data"
    return PreprocessConfig(
        source_swap_dir=Path(os.environ.get("QC_MERGE_SWAP_PATH") or str(data_root / "swap_lin")),
        source_spot_dir=Path(os.environ.get("QC_MERGE_SPOT_PATH") or str(data_root / "spot_lin")),
        output_root=Path(os.environ.get("QC_PREPROCESS_OUT_ROOT") or str(data_root / "preprocessed_hourly")),
        file_format=(os.environ.get("QC_PREPROCESS_FORMAT") or "parquet").lower(),
        compression=(os.environ.get("QC_PREPROCESS_COMPRESSION") or "zstd").lower(),
        include_optional_indicators=(os.environ.get("QC_PREPROCESS_INDICATORS", "1") != "0"),
    )


def preprocess_recent_hours(config: PreprocessConfig, *, hours: int = 1, end_anchor: datetime | None = None) -> dict[str, Any]:
    anchor = end_anchor or datetime.now(timezone.utc)
    end_hour = pd.Timestamp(anchor).floor("h") - pd.Timedelta(hours=1)
    start_hour = end_hour - pd.Timedelta(hours=max(0, int(hours) - 1))
    return preprocess_all(config, start_hour=start_hour, end_hour=end_hour)
