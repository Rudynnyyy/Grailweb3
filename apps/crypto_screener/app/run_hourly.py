from __future__ import annotations

import argparse
import sys
from pathlib import Path


repo_root = Path(__file__).resolve().parents[3]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))


from apps.crypto_screener.app.pipeline import (  # noqa: E402
    PipelinePaths,
    default_paths,
    run_forever,
    run_once,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["once", "forever"], default="once")
    parser.add_argument("--gamma-python", type=str, default=None)
    parser.add_argument("--out-dir", type=str, default=None)
    parser.add_argument("--no-fetch", action="store_true")
    args = parser.parse_args()

    paths = default_paths()
    if args.gamma_python:
        paths = PipelinePaths(**{**paths.__dict__, "gamma_python": args.gamma_python})
    if args.out_dir:
        paths = PipelinePaths(**{**paths.__dict__, "snapshot_out_dir": Path(args.out_dir)})

    fetch = not args.no_fetch
    if args.mode == "forever":
        run_forever(paths, fetch=fetch)
    else:
        run_once(paths, fetch=fetch)


if __name__ == "__main__":
    main()
