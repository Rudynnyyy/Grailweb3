from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _as_str(x) -> str:
    return "" if x is None else str(x)


def _is_num(x) -> bool:
    try:
        v = float(x)
        return v == v
    except Exception:
        return False


def _parse_iso(s: str) -> bool:
    try:
        datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        return True
    except Exception:
        return False


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", type=str, default="")
    ap.add_argument("--limit", type=int, default=3000)
    args = ap.parse_args()

    repo_root = _repo_root()
    p = Path(args.path).resolve() if args.path else (repo_root / "apps" / "crypto_screener" / "web" / "data" / "latest.json")

    out = {
        "path": str(p),
        "ok": True,
        "errors": [],
        "warnings": [],
        "stats": {},
    }

    if not p.exists():
        out["ok"] = False
        out["errors"].append("file_not_found")
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return

    try:
        raw = p.read_text(encoding="utf-8")
        j = json.loads(raw)
    except Exception as e:
        out["ok"] = False
        out["errors"].append(f"json_load_failed: {e}")
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return

    if not isinstance(j, dict):
        out["ok"] = False
        out["errors"].append("root_not_object")
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return

    summary = j.get("summary")
    config = j.get("config")
    results = j.get("results")

    if not isinstance(summary, dict):
        out["warnings"].append("summary_missing_or_not_object")
    if not isinstance(config, dict):
        out["warnings"].append("config_missing_or_not_object")
    if not isinstance(results, list):
        out["ok"] = False
        out["errors"].append("results_missing_or_not_array")
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return

    n = len(results)
    lim = max(1, int(args.limit))
    checked = 0
    bad_market = 0
    bad_symbol = 0
    bad_dt = 0
    bad_close = 0
    missing_series = 0
    series_len_min = None
    series_len_max = None
    key_dups = 0
    seen = set()

    for r in results[:lim]:
        checked += 1
        if not isinstance(r, dict):
            out["ok"] = False
            out["errors"].append("row_not_object")
            continue
        market = _as_str(r.get("market")).lower()
        symbol = _as_str(r.get("symbol")).upper()
        k = f"{market}|{symbol}"
        if k in seen:
            key_dups += 1
        else:
            seen.add(k)

        if market not in ("swap", "spot"):
            bad_market += 1
        if not symbol:
            bad_symbol += 1
        else:
            if not (symbol.endswith("-USDT") or symbol.endswith("USDT")):
                bad_symbol += 1

        dt_disp = r.get("dt_display")
        dt_close = r.get("dt_close")
        if dt_disp is not None and not _parse_iso(_as_str(dt_disp)):
            bad_dt += 1
        if dt_close is not None and not _parse_iso(_as_str(dt_close)):
            bad_dt += 1

        if "close" in r and not _is_num(r.get("close")):
            bad_close += 1

        s = r.get("series")
        if s is None:
            missing_series += 1
        elif isinstance(s, dict):
            close_s = s.get("close")
            if isinstance(close_s, list):
                sl = len(close_s)
                series_len_min = sl if series_len_min is None else min(series_len_min, sl)
                series_len_max = sl if series_len_max is None else max(series_len_max, sl)
        else:
            missing_series += 1

    if checked < n:
        out["warnings"].append(f"checked_partial: {checked}/{n}")

    if key_dups:
        out["warnings"].append(f"duplicate_market_symbol: {key_dups}")
    if bad_market:
        out["warnings"].append(f"bad_market_rows: {bad_market}")
    if bad_symbol:
        out["warnings"].append(f"bad_symbol_rows: {bad_symbol}")
    if bad_dt:
        out["warnings"].append(f"bad_dt_rows: {bad_dt}")
    if bad_close:
        out["warnings"].append(f"bad_close_rows: {bad_close}")

    out["stats"] = {
        "results_size": n,
        "checked": checked,
        "series_missing": missing_series,
        "series_close_len_min": series_len_min,
        "series_close_len_max": series_len_max,
        "summary_keys": sorted(list(summary.keys())) if isinstance(summary, dict) else [],
        "config_keys": sorted(list(config.keys())) if isinstance(config, dict) else [],
    }

    try:
        sum_size = summary.get("results_size") if isinstance(summary, dict) else None
        if isinstance(sum_size, int) and sum_size != n:
            out["warnings"].append(f"summary.results_size_mismatch: {sum_size}!={n}")
    except Exception:
        pass

    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
