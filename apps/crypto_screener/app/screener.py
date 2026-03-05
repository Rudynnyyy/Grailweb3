from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class ScreenerRuleConfig:
    tail_len: int = 360
    bar_hours: int = 1
    display_shift_hours: int = 0

    suggested_ma_windows: tuple[int, ...] = (5, 10, 20, 30, 60, 120)
    suggested_rsi_periods: tuple[int, ...] = (6, 14, 21)
    dt_tolerance_hours: int = 2

    cond_ma_fast: int = 10
    cond_ma_slow: int = 20
    cond_rsi_period: int = 14
    cond_rsi_threshold: float = 60.0


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def default_data_center_root(repo_root: Path | None = None) -> Path:
    repo_root = repo_root or _repo_root()
    return repo_root / "数据获取" / "data" / "data_center"


def _symbol_display(symbol: str) -> str:
    s = symbol.strip()
    if s.endswith("USDT") and "-" not in s:
        return f"{s[:-4]}-USDT"
    return s


def _is_trade_symbol(symbol: str) -> bool:
    s = symbol.upper().replace("-", "")
    if not s.endswith("USDT"):
        return False
    base = s[:-4]
    if base.endswith(("UP", "DOWN", "BEAR", "BULL")) and base != "JUP":
        return False
    return True


def _read_first_line_bytes(f) -> bytes:
    f.seek(0)
    return f.readline()


def _decode_bytes(raw: bytes) -> str:
    for enc in ("gbk", "utf-8", "latin-1", "cp1252"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


def _read_head_lines(path: Path, *, n: int = 3, max_bytes: int = 65536) -> list[str]:
    with open(path, "rb") as f:
        raw = f.read(max_bytes)
    text = _decode_bytes(raw)
    lines = [ln for ln in text.splitlines() if ln.strip() != ""]
    return lines[:n]


def _pick_header_line(head_lines: list[str]) -> tuple[str | None, int]:
    if not head_lines:
        return None, 0
    if "candle_begin_time" in head_lines[0]:
        return head_lines[0], 1
    if len(head_lines) >= 2 and "candle_begin_time" in head_lines[1]:
        return head_lines[1], 2
    return head_lines[0], 1


def _read_tail_lines(path: Path, *, max_lines: int = 1200, block_size: int = 65536) -> list[str]:
    if max_lines <= 0:
        return []
    head_lines = _read_head_lines(path, n=3)
    header_line, header_skip = _pick_header_line(head_lines)
    if not header_line:
        return []

    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        end = f.tell()
        if end == 0:
            return []
        buf = b""
        pos = end
        need = max_lines + 4
        while pos > 0 and buf.count(b"\n") < need:
            step = block_size if pos >= block_size else pos
            pos -= step
            f.seek(pos)
            buf = f.read(step) + buf
        raw = buf

    text = _decode_bytes(raw)
    lines = [ln for ln in text.splitlines() if ln.strip() != ""]
    if not lines:
        return []

    tail = lines[-max_lines:] if len(lines) > max_lines else lines
    tail = [ln for ln in tail if ln.strip() != header_line.strip()]
    return [header_line] + tail


def read_symbol_csv_tail(path: Path, *, max_lines: int = 1200) -> pd.DataFrame:
    lines = _read_tail_lines(path, max_lines=max_lines)
    if not lines:
        return pd.DataFrame()
    csv_text = "\n".join(lines)
    from io import StringIO

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
    if "candle_begin_time" not in df.columns:
        raise KeyError("缺少 candle_begin_time 列")
    out = df.copy()
    out["candle_begin_time"] = pd.to_datetime(out["candle_begin_time"], errors="coerce", utc=True, format="mixed")
    out = out.dropna(subset=["candle_begin_time"])
    out = out.sort_values("candle_begin_time")
    return out


def _latest_row(df: pd.DataFrame) -> pd.Series | None:
    if df.empty:
        return None
    last = df.iloc[-1]
    dt = pd.to_datetime(last.get("candle_begin_time", None), errors="coerce", utc=True, format="mixed")
    if pd.isna(dt):
        return None
    return last


def _to_float_list(s: pd.Series) -> list[float | None]:
    vals = pd.to_numeric(s, errors="coerce").tolist()
    out: list[float | None] = []
    for v in vals:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            out.append(None)
        else:
            out.append(float(v))
    return out


def _sma_last(closes: list[float | None], window: int) -> float | None:
    if window <= 0:
        return None
    arr = [v for v in closes if v is not None]
    if len(closes) < window:
        return None
    tail = closes[-window:]
    if any(v is None for v in tail):
        return None
    return float(sum(tail) / window)


def _rsi_last(closes: list[float | None], period: int) -> float | None:
    if period <= 0 or len(closes) < period + 1:
        return None
    xs = closes[-(period + 120) :] if len(closes) > period + 120 else closes
    ys = [v for v in xs if v is not None]
    if len(ys) < period + 1:
        return None

    gains: list[float] = []
    losses: list[float] = []
    for i in range(1, len(ys)):
        d = ys[i] - ys[i - 1]
        gains.append(d if d > 0 else 0.0)
        losses.append(-d if d < 0 else 0.0)

    if len(gains) < period:
        return None
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0 and avg_gain == 0:
        return 0.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100 - (100 / (1 + rs)))


def build_latest_snapshot_from_data_center(
    *,
    cfg: ScreenerRuleConfig | None = None,
    data_center_root: Path | None = None,
    fallback_swap_dir: Path | None = None,
    fallback_spot_dir: Path | None = None,
    fallback_min_symbols: int = 50,
    include_series: bool = False,
) -> dict[str, Any]:
    if cfg is None:
        tail_env = (os.environ.get("QC_SCREENER_TAIL_LEN") or "").strip()
        tail_val = None
        if tail_env:
            try:
                tail_val = int(tail_env)
            except Exception:
                tail_val = None
        if tail_val is None:
            tail_val = 360
        tail_val = max(80, min(2160, int(tail_val)))
        cfg = ScreenerRuleConfig(tail_len=int(tail_val))
    repo_root = _repo_root()
    data_center_root = data_center_root or default_data_center_root(repo_root)
    if os.name == "nt":
        _swap0 = r"D:\量化交易\数据\swap_lin"
        _spot0 = r"D:\量化交易\数据\spot_lin"
    else:
        _swap0 = str(repo_root / "数据获取" / "data" / "swap_lin")
        _spot0 = str(repo_root / "数据获取" / "data" / "spot_lin")
    fallback_swap_dir = fallback_swap_dir or Path(os.environ.get("QC_SCREENER_FALLBACK_SWAP_DIR") or _swap0)
    fallback_spot_dir = fallback_spot_dir or Path(os.environ.get("QC_SCREENER_FALLBACK_SPOT_DIR") or _spot0)

    rows: list[dict[str, Any]] = []

    def _fallback_symbol_filename(symbol_raw: str) -> str:
        s = symbol_raw.strip()
        if "-" in s:
            return f"{s}.csv" if not s.endswith(".csv") else s
        s2 = s.upper()
        if s2.endswith("USDT") and len(s2) > 4:
            return f"{s2[:-4]}-USDT.csv"
        return f"{s}.csv"

    def ingest_folder(folder: Path, market: str, *, symbol_from_filename: bool, fallback_dir: Path | None) -> None:
        if not folder.exists():
            return
        for csv_path in sorted(folder.glob("*.csv")):
            symbol_raw = csv_path.stem
            if symbol_from_filename and not _is_trade_symbol(symbol_raw):
                continue
            df = pd.DataFrame()
            for mul in (1, 2, 4):
                try:
                    df = read_symbol_csv_tail(csv_path, max_lines=cfg.tail_len * mul + 200)
                except Exception:
                    df = pd.DataFrame()
                if not df.empty and len(df) >= cfg.tail_len:
                    break
                if not df.empty and len(df) >= max(80, cfg.tail_len // 4):
                    break
            if df.empty or "close" not in df.columns:
                continue
            try:
                df = _ensure_dt(df)
            except Exception:
                continue
            df = df.tail(cfg.tail_len).copy()
            last = _latest_row(df)
            if last is None:
                continue

            symbol_disp = symbol_raw if "-" in symbol_raw else _symbol_display(symbol_raw)
            dt_begin = pd.to_datetime(last["candle_begin_time"], utc=True)
            dt_close = dt_begin + timedelta(hours=cfg.bar_hours)
            dt_display = dt_close + timedelta(hours=cfg.display_shift_hours)

            if fallback_dir and len(df) < cfg.tail_len:
                fb_file = fallback_dir / _fallback_symbol_filename(symbol_raw)
                if fb_file.exists():
                    try:
                        fb_df = read_symbol_csv_tail(fb_file, max_lines=cfg.tail_len + 50)
                        if not fb_df.empty and "close" in fb_df.columns:
                            fb_df = _ensure_dt(fb_df)
                            merged_df = pd.concat([fb_df, df], ignore_index=True)
                            merged_df = merged_df.drop_duplicates(subset=["candle_begin_time"], keep="last")
                            merged_df = merged_df.sort_values("candle_begin_time")
                            df = merged_df.tail(cfg.tail_len).copy()
                    except Exception:
                        pass

            if len(df) < 3:
                continue

            record: dict[str, Any] = {
                "symbol": symbol_disp,
                "market": market,
                "dt_begin": dt_begin.isoformat(),
                "dt_close": dt_close.isoformat(),
                "dt_display": dt_display.isoformat(),
                "close": float(last["close"]) if pd.notna(last["close"]) else None,
            }

            series_obj: dict[str, list[float | None]] = {}
            for col in ("open", "high", "low", "close", "volume", "quote_volume"):
                if col in df.columns:
                    series_obj[col] = _to_float_list(df[col])
                elif col == "quote_volume" and "quotevolume" in df.columns:
                    series_obj[col] = _to_float_list(df["quotevolume"])
            if include_series:
                record["series"] = series_obj

            closes = series_obj.get("close", [])
            if closes:
                record["_default_ma_fast"] = _sma_last(closes, cfg.cond_ma_fast)
                record["_default_ma_slow"] = _sma_last(closes, cfg.cond_ma_slow)
                record["_default_rsi"] = _rsi_last(closes, cfg.cond_rsi_period)
                ma_ps = sorted({int(x) for x in (cfg.suggested_ma_windows or ()) if int(x) > 0} | {int(cfg.cond_ma_fast), int(cfg.cond_ma_slow)})
                rsi_ps = sorted({int(x) for x in (cfg.suggested_rsi_periods or ()) if int(x) > 0} | {int(cfg.cond_rsi_period)})
                builtins: dict[str, float | None] = {}
                for p in ma_ps:
                    builtins[f"ma_{p}"] = _sma_last(closes, p)
                for p in rsi_ps:
                    builtins[f"rsi_{p}"] = _rsi_last(closes, p)
                record["_builtins"] = builtins

            rows.append(record)

    ingest_folder(data_center_root / "kline" / "swap", "swap", symbol_from_filename=True, fallback_dir=fallback_swap_dir)
    ingest_folder(data_center_root / "kline" / "spot", "spot", symbol_from_filename=True, fallback_dir=fallback_spot_dir)

    if len(rows) < fallback_min_symbols:
        ingest_folder(fallback_swap_dir, "swap", symbol_from_filename=False, fallback_dir=None)
        ingest_folder(fallback_spot_dir, "spot", symbol_from_filename=False, fallback_dir=None)

    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        key = (r.get("symbol", ""), r.get("market", ""))
        cur = merged.get(key)
        if not cur:
            merged[key] = r
            continue
        try:
            cur_dt = datetime.fromisoformat(str(cur.get("dt_close", "")).replace("Z", "+00:00"))
            new_dt = datetime.fromisoformat(str(r.get("dt_close", "")).replace("Z", "+00:00"))
        except Exception:
            merged[key] = r
            continue
        if new_dt > cur_dt:
            merged[key] = r
            continue
        if new_dt == cur_dt:
            cur_len = len((cur.get("series") or {}).get("close") or [])
            new_len = len((r.get("series") or {}).get("close") or [])
            if new_len > cur_len:
                merged[key] = r

    rows = list(merged.values())

    latest_dt_display = None
    latest_dt_close = None
    if rows:
        latest_dt_display = max(r["dt_display"] for r in rows if r.get("dt_display"))
        latest_dt_close = max(r["dt_close"] for r in rows if r.get("dt_close"))

    if not rows:
        now = datetime.now(timezone.utc).isoformat()
        return {
            "summary": {"generated_at": now, "latest_dt_display": None, "latest_dt_close": None, "universe_size": 0, "results_size": 0, "selected_size": 0},
            "config": asdict(cfg),
            "results": [],
        }

    latest_dt_dt = datetime.fromisoformat(str(latest_dt_close).replace("Z", "+00:00")) if latest_dt_close else None
    if latest_dt_dt is None:
        now = datetime.now(timezone.utc).isoformat()
        return {
            "summary": {"generated_at": now, "latest_dt_display": latest_dt_display, "latest_dt_close": latest_dt_close, "universe_size": len(rows), "results_size": len(rows), "selected_size": 0},
            "config": asdict(cfg),
            "results": rows,
        }

    min_keep = latest_dt_dt - timedelta(hours=cfg.dt_tolerance_hours)
    filtered_rows = rows
    fresh_size = 0
    for r in rows:
        try:
            r_dt = datetime.fromisoformat(str(r["dt_close"]).replace("Z", "+00:00"))
        except Exception:
            continue
        if r_dt >= min_keep:
            fresh_size += 1

    selected_rows = []
    for r in filtered_rows:
        close_v = r.get("close")
        ma_fast_v = r.get("_default_ma_fast")
        ma_slow_v = r.get("_default_ma_slow")
        rsi_v = r.get("_default_rsi")
        if close_v is None or ma_fast_v is None or ma_slow_v is None or rsi_v is None:
            continue
        if not (close_v > ma_slow_v and rsi_v > cfg.cond_rsi_threshold):
            continue
        selected_rows.append(r)

    selected_sorted = sorted(selected_rows, key=lambda x: (x.get("_default_rsi", -1e18), x.get("symbol", "")), reverse=True)
    for i, r in enumerate(selected_sorted, start=1):
        r["rank"] = i

    now = datetime.now(timezone.utc).isoformat()
    return {
        "summary": {
            "generated_at": now,
            "latest_dt_display": latest_dt_display,
            "latest_dt_close": latest_dt_close,
            "universe_size": len(rows),
            "results_size": len(filtered_rows),
            "selected_size": len(selected_rows),
            "fresh_size": fresh_size,
        },
        "config": asdict(cfg),
        "results": filtered_rows,
    }


def build_meta(cfg: ScreenerRuleConfig | None = None) -> dict[str, Any]:
    cfg = cfg or ScreenerRuleConfig()
    fields = [
        {"key": "rank", "name": "排名"},
        {"key": "symbol", "name": "币种"},
        {"key": "market", "name": "市场"},
        {"key": "dt_display", "name": "时间"},
        {"key": "close", "name": "收盘价"},
    ]

    return {
        "version": 1,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "fields": fields,
        "config": asdict(cfg),
        "default_sort": {"key": "close", "order": "desc"},
    }


def write_json_atomic(data: dict[str, Any], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    text = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    tmp_path.write_text(text, encoding="utf-8")
    tmp_path.replace(out_path)
