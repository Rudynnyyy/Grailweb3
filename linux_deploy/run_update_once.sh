#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

ENV_FILE="${QC_ENV_FILE:-$ROOT_DIR/linux_deploy/env.sh}"
if [ -f "$ENV_FILE" ]; then
  set -a
  . "$ENV_FILE"
  set +a
fi
if [ -f "/etc/qc_screener.env" ]; then
  set -a
  . "/etc/qc_screener.env"
  set +a
fi

export QC_GAMMA_PYTHON="${QC_GAMMA_PYTHON:-python3}"
export QC_LOCAL_SWAP_PATH="${QC_LOCAL_SWAP_PATH:-$ROOT_DIR/数据获取/data/swap_lin}"
export QC_MERGE_SWAP_PATH="${QC_MERGE_SWAP_PATH:-$ROOT_DIR/数据获取/data/swap_lin}"
export QC_LOCAL_SPOT_PATH="${QC_LOCAL_SPOT_PATH:-$ROOT_DIR/数据获取/data/spot_lin}"
export QC_MERGE_SPOT_PATH="${QC_MERGE_SPOT_PATH:-$ROOT_DIR/数据获取/data/spot_lin}"

mkdir -p "$ROOT_DIR/数据获取/data/swap_lin" "$ROOT_DIR/数据获取/data/spot_lin"

cd "$ROOT_DIR"
"$QC_GAMMA_PYTHON" -c "from apps.crypto_screener.app.pipeline import default_paths, run_once; run_once(default_paths(), fetch=True)"
"$QC_GAMMA_PYTHON" 数据获取/incremental_update.py --config 数据获取/config.yaml --once --lag-hours "${QC_PREPROCESS_LAG_HOURS:-1}" --max-hours "${QC_PREPROCESS_MAX_HOURS_PER_RUN:-24}"
if [ "${QC_BUILD_PKL_CACHE:-0}" != "0" ]; then
  "$QC_GAMMA_PYTHON" 数据获取/factor_cache_update.py --market all --tail "${QC_PKL_CACHE_TAIL:-2160}"
fi
