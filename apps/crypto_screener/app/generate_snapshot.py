from __future__ import annotations

import argparse
import gzip
import os
import sys
from pathlib import Path


repo_root = Path(__file__).resolve().parents[3]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))


from apps.crypto_screener.app.screener import (  # noqa: E402
    ScreenerRuleConfig,
    build_latest_snapshot_from_data_center,
    build_meta,
    default_data_center_root,
    write_json_atomic,
)


def _default_out_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "web" / "data"


def _write_gzip_atomic(*, src_path: Path, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    with src_path.open("rb") as f_in:
        with tmp_path.open("wb") as raw_out:
            with gzip.GzipFile(fileobj=raw_out, mode="wb", compresslevel=6, mtime=0) as f_out:
                while True:
                    buf = f_in.read(1024 * 1024)
                    if not buf:
                        break
                    f_out.write(buf)
    tmp_path.replace(out_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", type=str, default=str(_default_out_dir()))
    parser.add_argument("--data-center-root", type=str, default=str(default_data_center_root(repo_root)))
    parser.add_argument("--fallback-swap-dir", type=str, default=None)
    parser.add_argument("--fallback-spot-dir", type=str, default=None)
    parser.add_argument("--fallback-min-symbols", type=int, default=50)

    parser.add_argument("--tail-len", type=int, default=360)
    parser.add_argument("--bar-hours", type=int, default=1)
    parser.add_argument("--display-shift-hours", type=int, default=0)
    parser.add_argument("--suggested-ma-windows", type=str, default="5,10,20,30,60,120")
    parser.add_argument("--suggested-rsi-periods", type=str, default="6,14,21")
    parser.add_argument("--dt-tolerance-hours", type=int, default=2)

    parser.add_argument("--cond-ma-fast", type=int, default=10)
    parser.add_argument("--cond-ma-slow", type=int, default=20)
    parser.add_argument("--cond-rsi-period", type=int, default=14)
    parser.add_argument("--cond-rsi-threshold", type=float, default=60.0)

    parser.add_argument("--latest-only", action="store_true")
    parser.add_argument("--meta-only", action="store_true")
    args = parser.parse_args()

    suggested_ma_windows = tuple(int(x) for x in args.suggested_ma_windows.split(",") if x.strip())
    suggested_rsi_periods = tuple(int(x) for x in args.suggested_rsi_periods.split(",") if x.strip())

    cfg = ScreenerRuleConfig(
        tail_len=args.tail_len,
        bar_hours=args.bar_hours,
        display_shift_hours=args.display_shift_hours,
        suggested_ma_windows=suggested_ma_windows,
        suggested_rsi_periods=suggested_rsi_periods,
        dt_tolerance_hours=args.dt_tolerance_hours,
        cond_ma_fast=args.cond_ma_fast,
        cond_ma_slow=args.cond_ma_slow,
        cond_rsi_period=args.cond_rsi_period,
        cond_rsi_threshold=args.cond_rsi_threshold,
    )

    out_dir = Path(args.out_dir)
    meta_path = out_dir / "meta.json"
    latest_path = out_dir / "latest.json"

    if not args.latest_only:
        meta = build_meta(cfg)
        write_json_atomic(meta, meta_path)

    if not args.meta_only:
        latest = build_latest_snapshot_from_data_center(
            cfg=cfg,
            data_center_root=Path(args.data_center_root),
            fallback_swap_dir=Path(args.fallback_swap_dir) if args.fallback_swap_dir else None,
            fallback_spot_dir=Path(args.fallback_spot_dir) if args.fallback_spot_dir else None,
            fallback_min_symbols=args.fallback_min_symbols,
            include_series=(os.environ.get("QC_SCREENER_INCLUDE_SERIES") == "1"),
        )
        write_json_atomic(latest, latest_path)
        _write_gzip_atomic(src_path=latest_path, out_path=latest_path.with_suffix(".json.gz"))


if __name__ == "__main__":
    main()
