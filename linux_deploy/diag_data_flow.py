from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass
class CheckResult:
    ok: bool
    message: str
    detail: dict


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _symbol_candidates(symbol: str) -> list[str]:
    s = str(symbol or "").strip().upper()
    if not s:
        return []
    out: list[str] = []
    if s not in out:
        out.append(s)
    if s.endswith("-USDT"):
        s2 = s.replace("-", "")
        if s2 not in out:
            out.append(s2)
    if s.endswith("USDT") and not s.endswith("-USDT"):
        s2 = f"{s[:-4]}-USDT"
        if s2 not in out:
            out.append(s2)
    return out


def _decode_bytes(raw: bytes) -> str:
    for enc in ("gbk", "utf-8", "latin-1", "cp1252"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


def _parse_dt(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce", utc=True, format="mixed")
    n = int(len(dt))
    ok = int(dt.notna().sum())
    if n > 0 and ok < max(3, int(n * 0.2)):
        xs = pd.to_numeric(series, errors="coerce")
        x_ok = int(xs.notna().sum())
        if x_ok >= max(3, int(n * 0.8)):
            dt_ms = pd.to_datetime(xs, errors="coerce", utc=True, unit="ms")
            dt_s = pd.to_datetime(xs, errors="coerce", utc=True, unit="s")
            if int(dt_ms.notna().sum()) >= ok:
                dt = dt_ms
                ok = int(dt.notna().sum())
            if int(dt_s.notna().sum()) > ok:
                dt = dt_s
    return dt


def _read_tail_with_auto_sep(path: Path, max_lines: int) -> pd.DataFrame:
    with path.open("rb") as f:
        head = _decode_bytes(f.read(65536))
    head_lines = [x.strip() for x in head.splitlines() if x.strip()]
    header = ""
    for ln in head_lines[:10]:
        if "candle_begin_time" in ln:
            header = ln
            break
    if not header and head_lines:
        header = head_lines[0]
    with path.open("rb") as f:
        f.seek(0, os.SEEK_END)
        end = f.tell()
        pos = end
        buf = b""
        while pos > 0 and buf.count(b"\n") < max_lines + 5:
            step = 65536 if pos >= 65536 else pos
            pos -= step
            f.seek(pos)
            buf = f.read(step) + buf
    tail_text = _decode_bytes(buf)
    body_lines = [x.strip() for x in tail_text.splitlines() if x.strip()]
    body_lines = [x for x in body_lines if "candle_begin_time" not in x]
    csv_text = header + "\n" + "\n".join(body_lines[-max_lines:])
    try:
        df = pd.read_csv(pd.io.common.StringIO(csv_text), sep=None, engine="python", on_bad_lines="skip")
    except TypeError:
        df = pd.read_csv(pd.io.common.StringIO(csv_text), sep=None, engine="python")
    df.columns = [str(c).lstrip("\ufeff").strip() for c in list(df.columns)]
    return df


def _first_last(arr: list) -> tuple[str, str]:
    if not arr:
        return "", ""
    return str(arr[0]), str(arr[-1])


def _resolve_paths(repo_root: Path) -> dict[str, Path]:
    data_center_root = Path(os.environ.get("QC_DATA_CENTER_ROOT") or str(repo_root / "数据获取" / "data" / "data_center"))
    swap_merge = Path(
        os.environ.get("QC_MERGE_SWAP_PATH")
        or os.environ.get("QC_SCREENER_FALLBACK_SWAP_DIR")
        or str(repo_root / "数据获取" / "data" / "swap_lin")
    )
    spot_merge = Path(
        os.environ.get("QC_MERGE_SPOT_PATH")
        or os.environ.get("QC_SCREENER_FALLBACK_SPOT_DIR")
        or str(repo_root / "数据获取" / "data" / "spot_lin")
    )
    return {
        "repo_root": repo_root,
        "data_center_root": data_center_root,
        "dc_swap": data_center_root / "kline" / "swap",
        "dc_spot": data_center_root / "kline" / "spot",
        "merge_swap": swap_merge,
        "merge_spot": spot_merge,
        "latest_json": repo_root / "apps" / "crypto_screener" / "web" / "data" / "latest.json",
        "meta_json": repo_root / "apps" / "crypto_screener" / "web" / "data" / "meta.json",
    }


def _count_csv(p: Path) -> int:
    if not p.exists() or not p.is_dir():
        return 0
    return len(list(p.glob("*.csv")))


def _pick_symbol(paths: dict[str, Path], prefer_market: str, forced_symbol: str | None) -> tuple[str, str]:
    if forced_symbol:
        return prefer_market, forced_symbol.strip().upper()
    latest = paths["latest_json"]
    if latest.exists():
        try:
            j = json.loads(latest.read_text(encoding="utf-8"))
            rows = j.get("results") if isinstance(j, dict) else []
            if isinstance(rows, list):
                for r in rows:
                    if not isinstance(r, dict):
                        continue
                    m = str(r.get("market") or "").lower()
                    s = str(r.get("symbol") or "").upper()
                    if m in ("swap", "spot") and s:
                        if prefer_market == "all" or m == prefer_market:
                            return m, s
        except Exception:
            pass
    bases = [("swap", paths["merge_swap"]), ("spot", paths["merge_spot"])]
    for m, b in bases:
        if prefer_market != "all" and m != prefer_market:
            continue
        if b.exists():
            xs = sorted(b.glob("*.csv"))
            if xs:
                return m, xs[0].stem.upper()
    return ("swap" if prefer_market == "all" else prefer_market), ""


def _find_csv(base: Path, symbol: str) -> Path | None:
    for c in _symbol_candidates(symbol):
        p = (base / f"{c}.csv").resolve()
        if p.exists() and p.is_file():
            return p
    return None


def _probe_one_symbol(paths: dict[str, Path], market: str, symbol: str, tail: int) -> dict:
    base_merge = paths["merge_swap"] if market == "swap" else paths["merge_spot"]
    base_dc = paths["dc_swap"] if market == "swap" else paths["dc_spot"]
    p_merge = _find_csv(base_merge, symbol)
    p_dc = _find_csv(base_dc, symbol)
    p_use = p_merge or p_dc
    out = {
        "market": market,
        "symbol": symbol,
        "merge_csv": str(p_merge) if p_merge else None,
        "dc_csv": str(p_dc) if p_dc else None,
        "csv_ok": False,
        "dt_ok": 0,
        "dt_total": 0,
        "message": "",
    }
    if p_use is None:
        out["message"] = "csv_not_found"
        return out
    try:
        df = _read_tail_with_auto_sep(p_use, max_lines=max(1200, int(tail) + 300))
        if "candle_begin_time" not in df.columns:
            out["message"] = "no_candle_begin_time"
            return out
        dt = _parse_dt(df["candle_begin_time"])
        out["dt_ok"] = int(dt.notna().sum())
        out["dt_total"] = int(len(dt))
        out["csv_ok"] = out["dt_ok"] > 0
        if not out["csv_ok"]:
            out["message"] = "dt_all_invalid"
    except Exception as e:
        out["message"] = str(e)
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=str, default=str(_repo_root()))
    parser.add_argument("--market", type=str, default="all", choices=["all", "swap", "spot"])
    parser.add_argument("--symbol", type=str, default="")
    parser.add_argument("--tail", type=int, default=360)
    parser.add_argument("--scan-latest", action="store_true")
    parser.add_argument("--scan-limit", type=int, default=2000)
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from apps.crypto_screener.app.series_source import load_symbol_series

    paths = _resolve_paths(repo_root)
    market, symbol = _pick_symbol(paths, args.market, args.symbol or None)
    out: dict[str, object] = {
        "env": {
            "QC_DATA_CENTER_ROOT": os.environ.get("QC_DATA_CENTER_ROOT"),
            "QC_MERGE_SWAP_PATH": os.environ.get("QC_MERGE_SWAP_PATH"),
            "QC_MERGE_SPOT_PATH": os.environ.get("QC_MERGE_SPOT_PATH"),
            "QC_SCREENER_FALLBACK_SWAP_DIR": os.environ.get("QC_SCREENER_FALLBACK_SWAP_DIR"),
            "QC_SCREENER_FALLBACK_SPOT_DIR": os.environ.get("QC_SCREENER_FALLBACK_SPOT_DIR"),
        },
        "paths": {k: str(v) for k, v in paths.items()},
        "counts": {
            "dc_swap_csv": _count_csv(paths["dc_swap"]),
            "dc_spot_csv": _count_csv(paths["dc_spot"]),
            "merge_swap_csv": _count_csv(paths["merge_swap"]),
            "merge_spot_csv": _count_csv(paths["merge_spot"]),
        },
        "target": {"market": market, "symbol": symbol},
    }

    if not symbol:
        out["error"] = "无法自动选择symbol，请用 --symbol 手动指定"
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return

    base_merge = paths["merge_swap"] if market == "swap" else paths["merge_spot"]
    base_dc = paths["dc_swap"] if market == "swap" else paths["dc_spot"]
    p_merge = _find_csv(base_merge, symbol)
    p_dc = _find_csv(base_dc, symbol)
    out["symbol_probe"] = {
        "candidates": _symbol_candidates(symbol),
        "merge_csv": str(p_merge) if p_merge else None,
        "dc_csv": str(p_dc) if p_dc else None,
    }

    s = load_symbol_series(market=market, symbol=symbol, tail=max(30, int(args.tail)), repo_root=repo_root)
    if s is None:
        out["load_symbol_series"] = {"ok": False, "message": "load_symbol_series返回None"}
    else:
        closes = s.series.get("close") or []
        dt = s.dt or []
        first_dt, last_dt = _first_last(dt)
        out["load_symbol_series"] = {
            "ok": True,
            "symbol": s.symbol,
            "dt_len": len(dt),
            "close_len": len(closes),
            "first_dt": first_dt,
            "last_dt": last_dt,
            "close_last": closes[-1] if closes else None,
        }

    p_use = p_merge or p_dc
    if p_use:
        try:
            df = _read_tail_with_auto_sep(p_use, max_lines=max(1200, int(args.tail) + 300))
            has_col = "candle_begin_time" in df.columns
            if has_col:
                dt = _parse_dt(df["candle_begin_time"])
                ok = int(dt.notna().sum())
                total = int(len(dt))
                first = ""
                last = ""
                if ok > 0:
                    d2 = dt.dropna()
                    first = str(d2.iloc[0])
                    last = str(d2.iloc[-1])
                out["csv_parse_check"] = {
                    "ok": ok > 0,
                    "rows": int(len(df)),
                    "dt_ok": ok,
                    "dt_total": total,
                    "first_dt": first,
                    "last_dt": last,
                    "columns": [str(x) for x in list(df.columns)],
                }
            else:
                out["csv_parse_check"] = {
                    "ok": False,
                    "rows": int(len(df)),
                    "message": "缺少candle_begin_time列",
                    "columns": [str(x) for x in list(df.columns)],
                }
        except Exception as e:
            out["csv_parse_check"] = {"ok": False, "message": str(e)}
    else:
        out["csv_parse_check"] = {"ok": False, "message": "未找到对应CSV文件"}

    latest = paths["latest_json"]
    if latest.exists():
        try:
            j = json.loads(latest.read_text(encoding="utf-8"))
            rows = j.get("results") if isinstance(j, dict) else []
            size = len(rows) if isinstance(rows, list) else 0
            hit = None
            if isinstance(rows, list):
                for r in rows:
                    if not isinstance(r, dict):
                        continue
                    if str(r.get("market") or "").lower() == market and str(r.get("symbol") or "").upper() in _symbol_candidates(symbol):
                        hit = r
                        break
            out["latest_json_check"] = {
                "ok": True,
                "results_size": size,
                "target_in_results": hit is not None,
                "target_close": (hit or {}).get("close") if isinstance(hit, dict) else None,
            }
        except Exception as e:
            out["latest_json_check"] = {"ok": False, "message": str(e)}
    else:
        out["latest_json_check"] = {"ok": False, "message": "latest.json不存在"}

    if args.scan_latest and paths["latest_json"].exists():
        scan = {
            "enabled": True,
            "checked": 0,
            "ok": 0,
            "failed": 0,
            "failed_examples": [],
        }
        try:
            j2 = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
            rows2 = j2.get("results") if isinstance(j2, dict) else []
            if isinstance(rows2, list):
                limit = max(1, int(args.scan_limit))
                for r in rows2[:limit]:
                    if not isinstance(r, dict):
                        continue
                    m = str(r.get("market") or "").lower()
                    s2 = str(r.get("symbol") or "").upper()
                    if m not in ("swap", "spot") or not s2:
                        continue
                    if args.market != "all" and m != args.market:
                        continue
                    scan["checked"] += 1
                    p = _probe_one_symbol(paths, m, s2, int(args.tail))
                    if p.get("csv_ok"):
                        scan["ok"] += 1
                    else:
                        scan["failed"] += 1
                        if len(scan["failed_examples"]) < 20:
                            scan["failed_examples"].append(p)
        except Exception as e:
            scan["error"] = str(e)
        out["scan_latest"] = scan

    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
