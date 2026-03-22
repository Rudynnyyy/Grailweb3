#!/usr/bin/env bash
# deploy.sh — 一键部署脚本（前端热更新 + 后端最小停机重启）
# 用法：bash linux_deploy/deploy.sh [选项]
#   --frontend-only   只更新前端文件，不重启后端（真正零停机）
#   --backend-only    只重启后端
#   --skip-backup     跳过备份步骤
#   --help            显示帮助

set -euo pipefail

# ========== 配置 ==========
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WEB_DIR="$ROOT_DIR/apps/crypto_screener/web"
APP_DIR="$ROOT_DIR/apps/crypto_screener/app"
DATA_DIR="$ROOT_DIR/数据获取"
BACKUP_DIR="$ROOT_DIR/linux_deploy/.backups"
PM2_APP="run_web"

# ========== 参数解析 ==========
FRONTEND_ONLY=0
BACKEND_ONLY=0
SKIP_BACKUP=0

for arg in "$@"; do
  case $arg in
    --frontend-only) FRONTEND_ONLY=1 ;;
    --backend-only)  BACKEND_ONLY=1 ;;
    --skip-backup)   SKIP_BACKUP=1 ;;
    --help)
      echo "用法: bash linux_deploy/deploy.sh [--frontend-only] [--backend-only] [--skip-backup]"
      exit 0
      ;;
  esac
done

# ========== 工具函数 ==========
log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "[$(date '+%H:%M:%S')] ✓ $*"; }
fail() { echo "[$(date '+%H:%M:%S')] ✗ $*" >&2; exit 1; }

# ========== 备份 ==========
do_backup() {
  if [ "$SKIP_BACKUP" = "1" ]; then
    log "跳过备份"
    return
  fi
  local ts
  ts=$(date '+%Y%m%d_%H%M%S')
  local bak="$BACKUP_DIR/$ts"
  mkdir -p "$bak"
  # 只备份关键文件
  for f in \
    "$WEB_DIR/index.html" \
    "$WEB_DIR/app.js" \
    "$WEB_DIR/style.css" \
    "$APP_DIR/web_server.py" \
    "$APP_DIR/series_source.py" \
    "$DATA_DIR/0_一键执行获取合并.py" \
    "$DATA_DIR/1_kline_update.py"
  do
    [ -f "$f" ] && cp "$f" "$bak/" || true
  done
  ok "备份完成 → $bak"
  # 只保留最近10次备份
  ls -dt "$BACKUP_DIR"/*/  2>/dev/null | tail -n +11 | xargs rm -rf || true
}

# ========== 前端更新（无需重启）==========
deploy_frontend() {
  log "更新前端文件..."
  local changed=0
  for f in index.html app.js style.css kline.html backtest.html monitor.html; do
    local src="$ROOT_DIR/linux_deploy/upload/$f"
    local dst="$WEB_DIR/$f"
    if [ -f "$src" ]; then
      cp -f "$src" "$dst"
      ok "$f"
      changed=$((changed+1))
    fi
  done
  # 删除旧的gzip缓存，强制重新压缩
  find "$WEB_DIR" -name '*.gz' -delete 2>/dev/null || true
  if [ "$changed" = "0" ]; then
    log "linux_deploy/upload/ 中没有前端文件，跳过"
  else
    ok "前端更新完成（$changed 个文件），浏览器 Ctrl+Shift+R 刷新即可看到新版本"
  fi
}

# ========== 后端更新（需要重启）==========
deploy_backend() {
  log "更新后端文件..."
  local changed=0
  for f in web_server.py series_source.py filter_engine.py pipeline.py monitor.py; do
    local src="$ROOT_DIR/linux_deploy/upload/$f"
    local dst="$APP_DIR/$f"
    if [ -f "$src" ]; then
      cp -f "$src" "$dst"
      ok "$f"
      changed=$((changed+1))
    fi
  done
  for f in 0_一键执行获取合并.py 1_kline_update.py 2_币安数据合并.py factor_loader.py preprocess_fast.py factor_cache_update.py incremental_update.py; do
    local src="$ROOT_DIR/linux_deploy/upload/$f"
    local dst="$DATA_DIR/$f"
    if [ -f "$src" ]; then
      cp -f "$src" "$dst"
      ok "$f"
      changed=$((changed+1))
    fi
  done

  if [ "$changed" = "0" ]; then
    log "linux_deploy/upload/ 中没有后端文件，仅重启"
  fi

  # 重启 PM2
  log "重启 $PM2_APP..."
  if pm2 list | grep -q "$PM2_APP"; then
    pm2 restart "$PM2_APP" --update-env
  else
    fail "PM2 应用 $PM2_APP 未找到，请先手动启动"
  fi

  # 等待服务就绪（最多15秒）
  log "等待服务就绪..."
  local port
  port=$(pm2 env 0 2>/dev/null | grep QC_SCREENER_PORT | grep -o '[0-9]*' | head -1 || echo "8001")
  for i in $(seq 1 15); do
    if curl -sf "http://127.0.0.1:${port:-8001}/data/meta.json" >/dev/null 2>&1; then
      ok "服务已就绪（${i}秒）"
      break
    fi
    sleep 1
    if [ "$i" = "15" ]; then
      fail "服务15秒内未就绪，请检查日志：pm2 logs $PM2_APP --lines 30"
    fi
  done
}

# ========== 主流程 ==========
log "======= 开始部署 ======="
log "项目根目录: $ROOT_DIR"
log "上传目录:   $ROOT_DIR/linux_deploy/upload/"

mkdir -p "$ROOT_DIR/linux_deploy/upload"

do_backup

if [ "$BACKEND_ONLY" = "1" ]; then
  deploy_backend
elif [ "$FRONTEND_ONLY" = "1" ]; then
  deploy_frontend
else
  # 默认：先更新前端（零停机），再更新后端（短暂停机）
  deploy_frontend
  deploy_backend
fi

# 清空upload目录
rm -f "$ROOT_DIR/linux_deploy/upload"/* 2>/dev/null || true

log "======= 部署完成 ======="
pm2 status
