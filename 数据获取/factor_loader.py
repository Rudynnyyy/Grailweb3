from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

try:
    from .preprocess_fast import PreprocessConfig, _partition_dir, _safe_symbol, find_source_csv, preprocess_all, preprocess_frame
except ImportError:
    from preprocess_fast import PreprocessConfig, _partition_dir, _safe_symbol, find_source_csv, preprocess_all, preprocess_frame


FactorCallable = Callable[[pd.DataFrame], pd.Series]


@dataclass(frozen=True)
class FactorLoadConfig:
    preprocess: PreprocessConfig
    use_mmap: bool = True
    max_cache_items: int = 2048


@dataclass
class _CacheItem:
    mtime_ns: int
    rows: int
    frame: pd.DataFrame
    touched: float


def _rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / float(period), adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / float(period), adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss
    out = 100 - (100 / (1 + rs))
    out = out.where(avg_loss != 0, 100)
    out = out.where(avg_gain != 0, 0)
    return out


def compute_factor_vectorized(df: pd.DataFrame, *, factor: str, window: int = 20) -> pd.Series:
    close = pd.to_numeric(df["close"], errors="coerce")
    f = str(factor).lower()
    if f == "ret1":
        return close.pct_change()
    if f == "logret1":
        return np.log(close.replace(0, np.nan)).diff()
    if f == "ema":
        return close.ewm(span=int(window), adjust=False, min_periods=int(window)).mean()
    if f == "ma":
        return close.rolling(window=int(window), min_periods=int(window)).mean()
    if f == "rsi":
        return _rsi(close, int(window))
    if f == "zscore":
        mean = close.rolling(window=int(window), min_periods=int(window)).mean()
        std = close.rolling(window=int(window), min_periods=int(window)).std(ddof=0)
        return (close - mean) / std
    if f == "ma_bias":
        ma = close.rolling(window=int(window), min_periods=int(window)).mean()
        return close / ma - 1.0
    raise ValueError(f"不支持的因子: {factor}")


def compute_factor_loop(df: pd.DataFrame, *, factor: str, window: int = 20) -> pd.Series:
    xs = pd.to_numeric(df["close"], errors="coerce").tolist()
    out = [np.nan] * len(xs)
    w = int(window)
    f = str(factor).lower()
    if f in ("ma", "ma_bias", "zscore"):
        for i in range(w - 1, len(xs)):
            sub = xs[i - w + 1 : i + 1]
            if any(pd.isna(v) for v in sub):
                continue
            m = float(sum(sub) / w)
            if f == "ma":
                out[i] = m
            elif f == "ma_bias":
                out[i] = float(xs[i] / m - 1.0) if m != 0 else np.nan
            else:
                var = sum((float(v) - m) ** 2 for v in sub) / w
                s = float(np.sqrt(var))
                out[i] = float((xs[i] - m) / s) if s != 0 else np.nan
        return pd.Series(out, index=df.index)
    if f == "ret1":
        for i in range(1, len(xs)):
            a = xs[i - 1]
            b = xs[i]
            if pd.isna(a) or pd.isna(b) or a == 0:
                continue
            out[i] = float(b / a - 1.0)
        return pd.Series(out, index=df.index)
    if f == "logret1":
        for i in range(1, len(xs)):
            a = xs[i - 1]
            b = xs[i]
            if pd.isna(a) or pd.isna(b) or a <= 0 or b <= 0:
                continue
            out[i] = float(np.log(b) - np.log(a))
        return pd.Series(out, index=df.index)
    if f == "ema":
        alpha = 2.0 / (w + 1.0)
        ema = np.nan
        count = 0
        for i, v in enumerate(xs):
            if pd.isna(v):
                continue
            vv = float(v)
            count += 1
            if pd.isna(ema):
                ema = vv
            else:
                ema = alpha * vv + (1.0 - alpha) * float(ema)
            if count >= w:
                out[i] = float(ema)
        return pd.Series(out, index=df.index)
    if f == "rsi":
        return compute_factor_vectorized(df, factor="rsi", window=w)
    raise ValueError(f"不支持的因子: {factor}")


class FactorLoader:
    def __init__(self, cfg: FactorLoadConfig):
        self.cfg = cfg
        self._cache: dict[str, _CacheItem] = {}
        self.cache_hit = 0
        self.cache_miss = 0

    def _evict_if_needed(self) -> None:
        max_items = int(self.cfg.max_cache_items)
        if len(self._cache) <= max_items:
            return
        items = sorted(self._cache.items(), key=lambda kv: kv[1].touched)
        drop = len(self._cache) - max_items
        for i in range(drop):
            self._cache.pop(items[i][0], None)

    def _load_partition_file(self, path: Path, columns: list[str] | None = None) -> pd.DataFrame:
        key = str(path.resolve())
        mtime_ns = int(path.stat().st_mtime_ns)
        hit = self._cache.get(key)
        if hit and hit.mtime_ns == mtime_ns:
            hit.touched = time.time()
            self.cache_hit += 1
            if columns:
                keep = [c for c in columns if c in hit.frame.columns]
                return hit.frame[keep].copy()
            return hit.frame.copy()
        self.cache_miss += 1
        ext = path.suffix.lower()
        if ext == ".parquet":
            frame = self._read_parquet(path, columns=columns)
        elif ext == ".pkl":
            frame = pd.read_pickle(path)
            if columns:
                keep = [c for c in columns if c in frame.columns]
                frame = frame[keep].copy()
        else:
            frame = pd.DataFrame()
        self._cache[key] = _CacheItem(mtime_ns=mtime_ns, rows=len(frame), frame=frame.copy(), touched=time.time())
        self._evict_if_needed()
        return frame

    def _read_parquet(self, path: Path, columns: list[str] | None = None) -> pd.DataFrame:
        if self.cfg.use_mmap:
            try:
                import pyarrow.parquet as pq

                table = pq.read_table(path, columns=columns, memory_map=True)
                return table.to_pandas(use_threads=True, split_blocks=True, self_destruct=True)
            except Exception:
                pass
        return pd.read_parquet(path, columns=columns)

    def _partition_files(self, market: str, symbol: str, dt_hour: pd.Timestamp) -> list[Path]:
        pdir = _partition_dir(self.cfg.preprocess.output_root, market, dt_hour)
        if not pdir.exists():
            return []
        sym = _safe_symbol(symbol)
        cands = [pdir / f"{sym}.parquet", pdir / f"{sym}.pkl"]
        return [p for p in cands if p.exists()]

    def _ensure_partition(self, market: str, symbol: str, start_hour: pd.Timestamp, end_hour: pd.Timestamp) -> None:
        src_dir = self.cfg.preprocess.source_swap_dir if market == "swap" else self.cfg.preprocess.source_spot_dir
        csv_path = find_source_csv(src_dir, symbol)
        if csv_path is None:
            return
        preprocess_all(
            self.cfg.preprocess,
            start_hour=start_hour,
            end_hour=end_hour,
        )

    def load_preprocessed_range(
        self,
        *,
        market: str,
        symbol: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        market_l = str(market).lower()
        st = pd.to_datetime(start, utc=True).floor("h")
        et = pd.to_datetime(end, utc=True).floor("h")
        blocks: list[pd.DataFrame] = []
        miss_hours: list[pd.Timestamp] = []
        for h in pd.date_range(st, et, freq="h"):
            files = self._partition_files(market_l, symbol, h)
            if not files:
                miss_hours.append(pd.to_datetime(h, utc=True))
                continue
            frame = self._load_partition_file(files[0], columns=columns)
            if not frame.empty:
                blocks.append(frame)
        if miss_hours:
            self._ensure_partition(market_l, symbol, min(miss_hours), max(miss_hours))
            for h in miss_hours:
                files = self._partition_files(market_l, symbol, h)
                if not files:
                    continue
                frame = self._load_partition_file(files[0], columns=columns)
                if not frame.empty:
                    blocks.append(frame)
        if not blocks:
            return pd.DataFrame()
        out = pd.concat(blocks, ignore_index=True)
        out["dt"] = pd.to_datetime(out["dt"], utc=True, errors="coerce")
        out = out.dropna(subset=["dt"])
        out = out[(out["dt"] >= pd.to_datetime(start, utc=True)) & (out["dt"] <= pd.to_datetime(end, utc=True))]
        out = out.sort_values("dt")
        out = out.drop_duplicates(subset=["dt"], keep="last")
        return out

    def factor_entry(
        self,
        *,
        market: str,
        symbol: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
        factor: str = "ma_bias",
        window: int = 20,
    ) -> pd.DataFrame:
        base_cols = ["dt", "symbol", "market", "close"]
        frame = self.load_preprocessed_range(
            market=market,
            symbol=symbol,
            start=start,
            end=end,
            columns=base_cols,
        )
        if frame.empty:
            src_dir = self.cfg.preprocess.source_swap_dir if str(market).lower() == "swap" else self.cfg.preprocess.source_spot_dir
            csv_path = find_source_csv(src_dir, symbol)
            if csv_path is None:
                return pd.DataFrame()
            raw = pd.read_csv(csv_path, encoding="gbk")
            frame = preprocess_frame(raw, market=str(market).lower(), symbol=symbol, include_optional_indicators=False)
            frame = frame[(frame["dt"] >= pd.to_datetime(start, utc=True)) & (frame["dt"] <= pd.to_datetime(end, utc=True))]
        frame = frame.sort_values("dt").copy()
        frame["factor_value"] = compute_factor_vectorized(frame, factor=factor, window=window)
        return frame


def load_config_from_yaml(path: Path, *, repo_root: Path) -> FactorLoadConfig:
    text = path.read_text(encoding="utf-8")
    data: dict[str, Any] = {}
    try:
        import yaml

        y = yaml.safe_load(text)
        data = y if isinstance(y, dict) else {}
    except Exception:
        try:
            data = json.loads(text)
        except Exception:
            data = {}
    pp = data.get("preprocess", {}) if isinstance(data, dict) else {}
    source_swap_dir = Path(os.environ.get("QC_MERGE_SWAP_PATH") or pp.get("source_swap_dir") or str(repo_root / "数据获取" / "data" / "swap_lin"))
    source_spot_dir = Path(os.environ.get("QC_MERGE_SPOT_PATH") or pp.get("source_spot_dir") or str(repo_root / "数据获取" / "data" / "spot_lin"))
    output_root = Path(os.environ.get("QC_PREPROCESS_OUT_ROOT") or pp.get("output_root") or str(repo_root / "数据获取" / "data" / "preprocessed_hourly"))
    if not source_swap_dir.exists():
        source_swap_dir = repo_root / "数据获取" / "data" / "swap_lin"
    if not source_spot_dir.exists():
        source_spot_dir = repo_root / "数据获取" / "data" / "spot_lin"
    fmt = str(pp.get("file_format") or "parquet").lower()
    compression = str(pp.get("compression") or "zstd").lower()
    p_cfg = PreprocessConfig(
        source_swap_dir=source_swap_dir,
        source_spot_dir=source_spot_dir,
        output_root=output_root,
        file_format=fmt,
        compression=compression,
        include_optional_indicators=bool(pp.get("include_optional_indicators", True)),
    )
    fl = data.get("factor_loader", {}) if isinstance(data, dict) else {}
    return FactorLoadConfig(
        preprocess=p_cfg,
        use_mmap=bool(fl.get("use_mmap", True)),
        max_cache_items=int(fl.get("max_cache_items", 2048)),
    )
