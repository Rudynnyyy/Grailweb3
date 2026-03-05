#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export QC_GAMMA_PYTHON="${QC_GAMMA_PYTHON:-python3}"
export QC_LOCAL_SWAP_PATH="${QC_LOCAL_SWAP_PATH:-$ROOT_DIR/数据获取/data/swap_lin}"
export QC_MERGE_SWAP_PATH="${QC_MERGE_SWAP_PATH:-$ROOT_DIR/数据获取/data/swap_lin}"
export QC_LOCAL_SPOT_PATH="${QC_LOCAL_SPOT_PATH:-$ROOT_DIR/数据获取/data/spot_lin}"
export QC_MERGE_SPOT_PATH="${QC_MERGE_SPOT_PATH:-$ROOT_DIR/数据获取/data/spot_lin}"

mkdir -p "$ROOT_DIR/数据获取/data/swap_lin" "$ROOT_DIR/数据获取/data/spot_lin"

cd "$ROOT_DIR"
python3 -c "from apps.crypto_screener.app.pipeline import default_paths, run_once; run_once(default_paths(), fetch=True)"
