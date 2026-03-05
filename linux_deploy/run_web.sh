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

export QC_SCREENER_HOST="${QC_SCREENER_HOST:-0.0.0.0}"
export QC_SCREENER_PORT="${QC_SCREENER_PORT:-8001}"
export QC_SCREENER_PORT_STRICT="${QC_SCREENER_PORT_STRICT:-1}"
export QC_BOOTSTRAP_UPDATE="${QC_BOOTSTRAP_UPDATE:-0}"

export QC_GAMMA_PYTHON="${QC_GAMMA_PYTHON:-python3}"

have_csv_dir() { [ -d "${1:-}" ] && ls "${1:-}"/*.csv >/dev/null 2>&1; }
repo_swap="$ROOT_DIR/数据获取/data/swap_lin"
repo_spot="$ROOT_DIR/数据获取/data/spot_lin"

export QC_LOCAL_SWAP_PATH="${QC_LOCAL_SWAP_PATH:-$repo_swap}"
export QC_LOCAL_SPOT_PATH="${QC_LOCAL_SPOT_PATH:-$repo_spot}"

if have_csv_dir "${QC_MERGE_SWAP_PATH:-}"; then
  export QC_MERGE_SWAP_PATH="$QC_MERGE_SWAP_PATH"
else
  export QC_MERGE_SWAP_PATH="$repo_swap"
fi
if have_csv_dir "${QC_MERGE_SPOT_PATH:-}"; then
  export QC_MERGE_SPOT_PATH="$QC_MERGE_SPOT_PATH"
else
  export QC_MERGE_SPOT_PATH="$repo_spot"
fi

if have_csv_dir "${QC_SCREENER_FALLBACK_SWAP_DIR:-}"; then
  export QC_SCREENER_FALLBACK_SWAP_DIR="$QC_SCREENER_FALLBACK_SWAP_DIR"
else
  export QC_SCREENER_FALLBACK_SWAP_DIR="$repo_swap"
fi
if have_csv_dir "${QC_SCREENER_FALLBACK_SPOT_DIR:-}"; then
  export QC_SCREENER_FALLBACK_SPOT_DIR="$QC_SCREENER_FALLBACK_SPOT_DIR"
else
  export QC_SCREENER_FALLBACK_SPOT_DIR="$repo_spot"
fi

mkdir -p "$ROOT_DIR/数据获取/data/swap_lin" "$ROOT_DIR/数据获取/data/spot_lin"

cd "$ROOT_DIR"
if [ ! -f "$ROOT_DIR/apps/crypto_screener/web/data/latest.json" ]; then
  echo "[run_web] latest.json not found; generating snapshot once..."
  python3 -c "import sys; sys.path.insert(0, r'$ROOT_DIR'); from apps.crypto_screener.app.pipeline import run_once, default_paths; run_once(default_paths(), fetch=False)"
fi
exec python3 apps/crypto_screener/app/web_server.py
