from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


repo_root = _repo_root()
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))


from apps.crypto_screener.app.series_source import load_symbol_series  # noqa: E402
from apps.crypto_screener.app.filter_engine import stoch_rsi  # noqa: E402
from apps.crypto_screener.app.expr_lang import eval_expression_scalar  # noqa: E402


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _dt(x: Any) -> datetime | None:
    if not x:
        return None
    try:
        return datetime.fromisoformat(str(x).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _pick_rows(latest: dict, n: int, market: str) -> list[dict]:
    rows = latest.get("results") if isinstance(latest, dict) else None
    if not isinstance(rows, list):
        return []
    out = []
    mk = str(market or "all").strip().lower()
    for r in rows:
        if not isinstance(r, dict):
            continue
        m = str(r.get("market") or "").strip().lower()
        if mk in ("spot", "swap") and m != mk:
            continue
        if m not in ("spot", "swap"):
            continue
        s = str(r.get("symbol") or "").strip()
        if not s:
            continue
        out.append(r)
        if len(out) >= int(n):
            break
    return out


def _series_latest(series: dict[str, Any]) -> dict[str, Any]:
    out = {}
    for k in ("open", "high", "low", "close", "volume", "quote_volume"):
        arr = series.get(k)
        v = None
        try:
            if isinstance(arr, list) and arr:
                for x in reversed(arr):
                    if x is None:
                        continue
                    try:
                        fx = float(x)
                    except Exception:
                        continue
                    if fx == fx:
                        v = fx
                        break
        except Exception:
            v = None
        out[k] = v
    return out


def _eval_expr(expr: str, series: dict[str, Any], latest0: dict[str, Any]) -> float | None:
    try:
        v = eval_expression_scalar(expr=str(expr), series=series, latest=latest0)
    except Exception:
        return None
    try:
        if v is None:
            return None
        x = float(v)
        return x if x == x else None
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--latest", type=str, default=str(repo_root / "apps" / "crypto_screener" / "web" / "data" / "latest.json"))
    ap.add_argument("--meta", type=str, default=str(repo_root / "apps" / "crypto_screener" / "web" / "data" / "meta.json"))
    ap.add_argument("--n", type=int, default=40)
    ap.add_argument("--market", type=str, default="swap")
    ap.add_argument("--symbols", type=str, default="")
    ap.add_argument("--tail", type=int, default=720)
    ap.add_argument("--expr", type=str, default="quotevolume/quotevolume.ma(20)")
    ap.add_argument("--rsi_p", type=int, default=14)
    ap.add_argument("--stoch_p", type=int, default=14)
    ap.add_argument("--smooth_k", type=int, default=3)
    ap.add_argument("--smooth_d", type=int, default=3)
    args = ap.parse_args()

    latest_path = Path(args.latest)
    meta_path = Path(args.meta)
    latest = _read_json(latest_path)
    meta = _read_json(meta_path)

    print("repo_root:", str(repo_root))
    print("latest_path:", str(latest_path), "exists:", latest_path.exists(), "size:", (latest_path.stat().st_size if latest_path.exists() else None))
    print("meta_path:", str(meta_path), "exists:", meta_path.exists(), "size:", (meta_path.stat().st_size if meta_path.exists() else None))
    print("env: QC_USE_PREPROCESSED_SERIES=", os.environ.get("QC_USE_PREPROCESSED_SERIES"), "QC_USE_PKL_SERIES_CACHE=", os.environ.get("QC_USE_PKL_SERIES_CACHE"), "QC_PKL_CACHE_TAIL=", os.environ.get("QC_PKL_CACHE_TAIL"))

    if not isinstance(latest, dict) or not isinstance(latest.get("results"), list):
        print("bad_latest_json")
        return 2

    summary = latest.get("summary") if isinstance(latest.get("summary"), dict) else {}
    print("snapshot.generated_at:", summary.get("generated_at"))
    print("snapshot.latest_dt_close:", summary.get("latest_dt_close"))
    print("snapshot.results_size:", summary.get("results_size"), "universe_size:", summary.get("universe_size"))

    sym_list = [s.strip() for s in str(args.symbols or "").replace(",", " ").split() if s.strip()]
    if sym_list:
        rows = [{"market": args.market, "symbol": s} for s in sym_list]
    else:
        rows = _pick_rows(latest, n=args.n, market=args.market)

    print("check.market:", args.market, "rows:", len(rows), "tail:", int(args.tail))
    print("check.stoch_params:", int(args.rsi_p), int(args.stoch_p), int(args.smooth_k), int(args.smooth_d))
    print("check.expr:", str(args.expr))

    for r in rows:
        market = str(r.get("market") or "").strip().lower()
        symbol = str(r.get("symbol") or "").strip()
        if not market or not symbol:
            continue
        s = load_symbol_series(market=market, symbol=symbol, tail=int(args.tail), repo_root=repo_root)
        if s is None:
            print(market, symbol, "series=None")
            continue
        src = str(getattr(s, "source", "") or "")
        dt_list = getattr(s, "dt", []) or []
        series = getattr(s, "series", {}) or {}
        closes = series.get("close") if isinstance(series, dict) else None
        qv = series.get("quote_volume") if isinstance(series, dict) else None
        latest0 = _series_latest(series if isinstance(series, dict) else {})
        kdj = stoch_rsi(list(closes or []), int(args.rsi_p), int(args.stoch_p), int(args.smooth_k), int(args.smooth_d))
        expr_v = _eval_expr(str(args.expr), series if isinstance(series, dict) else {}, latest0)
        dt_last = dt_list[-1] if isinstance(dt_list, list) and dt_list else None
        print(
            market,
            symbol,
            "source=" + src,
            "dt_last=" + str(dt_last),
            "len=" + str(len(dt_list) if isinstance(dt_list, list) else 0),
            "close_ok=" + str(latest0.get("close") is not None),
            "qv_len=" + str(len(qv) if isinstance(qv, list) else 0),
            "stoch_k=" + str(kdj.get("k")),
            "stoch_d=" + str(kdj.get("d")),
            "expr=" + str(expr_v),
        )

    if isinstance(meta, dict):
        print("meta.updated_at:", meta.get("updated_at"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
