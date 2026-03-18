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

# 两阶段数据更新流程
# 第一阶段：获取CSV数据 + 生成快照（3-4分钟）
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 第一阶段：获取CSV数据..."
"$QC_GAMMA_PYTHON" -c "from apps.crypto_screener.app.pipeline import default_paths, run_once_two_stage; run_once_two_stage(default_paths(), fetch=True)"

# 第二阶段：后台生成PKL预处理数据（8-9分钟）
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 第二阶段：生成PKL预处理数据..."
"$QC_GAMMA_PYTHON" 数据获取/incremental_update.py --config 数据获取/config.yaml --once --lag-hours "${QC_PREPROCESS_LAG_HOURS:-1}" --max-hours "${QC_PREPROCESS_MAX_HOURS_PER_RUN:-24}"

if [ "${QC_BUILD_PKL_CACHE:-0}" != "0" ]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] 生成PKL缓存..."
  "$QC_GAMMA_PYTHON" 数据获取/factor_cache_update.py --market all --tail "${QC_PKL_CACHE_TAIL:-2160}"
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 数据更新完成"
