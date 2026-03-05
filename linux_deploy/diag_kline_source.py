from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _count_lines_fast(path: Path, max_bytes: int = 0) -> int:
    n = 0
    with path.open("rb") as f:
        if max_bytes and max_bytes > 0:
            buf = f.read(int(max_bytes))
            return int(buf.count(b"\n"))
        while True:
            buf = f.read(1024 * 1024)
            if not buf:
                break
            n += int(buf.count(b"\n"))
    return n


def _read_tail_lines(path: Path, n: int = 5) -> list[str]:
    try:
        with path.open("rb") as f:
            f.seek(0, os.SEEK_END)
            end = f.tell()
            pos = end
            buf = b""
            while pos > 0 and buf.count(b"\n") < n + 5:
                step = 65536 if pos >= 65536 else pos
                pos -= step
                f.seek(pos)
                buf = f.read(step) + buf
        try:
            text = buf.decode("utf-8")
        except Exception:
            text = buf.decode("gbk", errors="ignore")
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        return lines[-n:]
    except Exception:
        return []


def _parse_dt_series(series):
    import pandas as pd

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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-root", type=str, default=str(_repo_root()))
    ap.add_argument("--market", type=str, required=True, choices=["swap", "spot"])
    ap.add_argument("--symbol", type=str, required=True)
    ap.add_argument("--tail", type=int, default=720)
    args = ap.parse_args()

    repo_root = Path(args.repo_root).resolve()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from apps.crypto_screener.app import series_source as ss  # noqa

    market = str(args.market).lower()
    symbol = str(args.symbol).strip().upper()
    tail = max(30, min(3650, int(args.tail)))

    swap_dir, spot_dir = ss._default_merge_dirs(repo_root)  # type: ignore[attr-defined]
    dc_swap_dir, dc_spot_dir = ss._default_data_center_dirs(repo_root)  # type: ignore[attr-defined]
    base_merge = swap_dir if market == "swap" else spot_dir
    base_dc = dc_swap_dir if market == "swap" else dc_spot_dir

    cands = ss._symbol_candidates(symbol)  # type: ignore[attr-defined]
    csv_path, picked_sym = ss._pick_existing_csv([base_merge, base_dc], symbol)  # type: ignore[attr-defined]

    out: dict[str, object] = {
        "repo_root": str(repo_root),
        "env": {
            "QC_MERGE_SWAP_PATH": os.environ.get("QC_MERGE_SWAP_PATH"),
            "QC_MERGE_SPOT_PATH": os.environ.get("QC_MERGE_SPOT_PATH"),
            "QC_SCREENER_FALLBACK_SWAP_DIR": os.environ.get("QC_SCREENER_FALLBACK_SWAP_DIR"),
            "QC_SCREENER_FALLBACK_SPOT_DIR": os.environ.get("QC_SCREENER_FALLBACK_SPOT_DIR"),
            "QC_DATA_CENTER_ROOT": os.environ.get("QC_DATA_CENTER_ROOT"),
        },
        "resolved_dirs": {
            "merge_swap": str(swap_dir),
            "merge_spot": str(spot_dir),
            "dc_swap": str(dc_swap_dir),
            "dc_spot": str(dc_spot_dir),
        },
        "request": {"market": market, "symbol": symbol, "tail": tail},
        "symbol_candidates": cands,
        "picked": {
            "picked_sym": picked_sym,
            "csv_path": str(csv_path) if csv_path else None,
            "picked_from": None,
        },
    }

    if csv_path:
        try:
            base_merge_r = base_merge.resolve()
            base_dc_r = base_dc.resolve()
            p_r = csv_path.resolve()
            if base_merge_r in p_r.parents or p_r == base_merge_r:
                out["picked"]["picked_from"] = "merge"
            elif base_dc_r in p_r.parents or p_r == base_dc_r:
                out["picked"]["picked_from"] = "data_center"
        except Exception:
            pass

        try:
            st = csv_path.stat()
            out["csv_stat"] = {
                "size_bytes": int(st.st_size),
                "mtime": int(st.st_mtime),
                "lines_est": _count_lines_fast(csv_path),
                "tail_lines": _read_tail_lines(csv_path, n=5),
            }
        except Exception as e:
            out["csv_stat"] = {"error": str(e)}

        try:
            df = ss.read_merge_csv_tail(csv_path, tail=tail)  # type: ignore[attr-defined]
            out["read_merge_csv_tail"] = {
                "rows": int(len(df)),
                "columns": [str(c) for c in list(df.columns)],
            }
            if "candle_begin_time" in df.columns:
                dt = _parse_dt_series(df["candle_begin_time"])
                ok = int(dt.notna().sum())
                out["read_merge_csv_tail"]["dt_ok"] = ok
                out["read_merge_csv_tail"]["dt_total"] = int(len(dt))
                if ok:
                    d2 = dt.dropna()
                    out["read_merge_csv_tail"]["first_dt"] = str(d2.iloc[0])
                    out["read_merge_csv_tail"]["last_dt"] = str(d2.iloc[-1])
        except Exception as e:
            out["read_merge_csv_tail"] = {"error": str(e)}

    try:
        s0 = ss.load_symbol_series(market=market, symbol=symbol, tail=tail, repo_root=repo_root)
        if s0 is None:
            out["load_symbol_series"] = {"ok": False, "message": "returned_none"}
        else:
            closes = s0.series.get("close") or []
            out["load_symbol_series"] = {
                "ok": True,
                "symbol": s0.symbol,
                "dt_len": len(s0.dt or []),
                "close_len": len(closes),
                "first_dt": str((s0.dt or [""])[0]) if (s0.dt or []) else "",
                "last_dt": str((s0.dt or [""])[-1]) if (s0.dt or []) else "",
                "close_last": closes[-1] if closes else None,
            }
    except Exception as e:
        out["load_symbol_series"] = {"ok": False, "error": str(e)}

    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
