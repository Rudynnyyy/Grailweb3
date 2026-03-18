"""
两阶段Web服务器启动脚本
- 第一阶段：CSV数据快速展示（3-4分钟）
- 第二阶段：PKL预处理数据后台生成（8-9分钟）

前端通过 /api/data_source_status 检测数据源状态，自动切换
"""

from __future__ import annotations

import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from subprocess import Popen, PIPE

repo_root = Path(__file__).resolve().parents[3]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))


def _mark_csv_ready(repo_root: Path) -> None:
    """标记CSV快照已就绪"""
    marker = repo_root / "apps" / "crypto_screener" / "web" / "data" / ".csv_ready"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(datetime.now(timezone.utc).isoformat())
    print(f"[CSV READY] {marker}")


def _mark_pkl_ready(repo_root: Path) -> None:
    """标记PKL预处理数据已就绪"""
    marker = repo_root / "apps" / "crypto_screener" / "web" / "data" / ".pkl_ready"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(datetime.now(timezone.utc).isoformat())
    print(f"[PKL READY] {marker}")


def _run_pkl_build_background(repo_root: Path, gamma_python: str) -> None:
    """后台生成PKL预处理数据"""
    try:
        data_fetch_dir = repo_root / "数据获取"
        
        # 运行增量预处理
        print("[PKL BUILD] Starting incremental_update.py...")
        cmd = [
            gamma_python,
            str(data_fetch_dir / "incremental_update.py"),
            "--config", str(data_fetch_dir / "config.yaml"),
            "--once",
            "--lag-hours", os.environ.get("QC_PREPROCESS_LAG_HOURS", "1"),
            "--max-hours", os.environ.get("QC_PREPROCESS_MAX_HOURS_PER_RUN", "24"),
        ]
        proc = Popen(cmd, cwd=str(repo_root), stdout=PIPE, stderr=PIPE)
        stdout, stderr = proc.communicate()
        
        if proc.returncode != 0:
            print(f"[PKL BUILD ERROR] incremental_update.py failed: {stderr.decode('utf-8', errors='ignore')}")
            return
        
        print("[PKL BUILD] incremental_update.py completed")
        
        # 运行PKL缓存生成
        if os.environ.get("QC_BUILD_PKL_CACHE", "0") != "0":
            print("[PKL BUILD] Starting factor_cache_update.py...")
            cmd = [
                gamma_python,
                str(data_fetch_dir / "factor_cache_update.py"),
                "--market", "all",
                "--tail", os.environ.get("QC_PKL_CACHE_TAIL", "2160"),
            ]
            proc = Popen(cmd, cwd=str(repo_root), stdout=PIPE, stderr=PIPE)
            stdout, stderr = proc.communicate()
            
            if proc.returncode != 0:
                print(f"[PKL BUILD ERROR] factor_cache_update.py failed: {stderr.decode('utf-8', errors='ignore')}")
                return
            
            print("[PKL BUILD] factor_cache_update.py completed")
        
        # 标记PKL已就绪
        _mark_pkl_ready(repo_root)
        print("[PKL BUILD] PKL data is ready for use")
        
    except Exception as e:
        print(f"[PKL BUILD ERROR] {e}")


def main() -> None:
    """启动Web服务器并管理两阶段数据流"""
    gamma_python = os.environ.get("QC_GAMMA_PYTHON", "python3")
    
    # 标记CSV已就绪
    _mark_csv_ready(repo_root)
    
    # 启动后台PKL生成线程
    pkl_thread = threading.Thread(
        target=_run_pkl_build_background,
        args=(repo_root, gamma_python),
        daemon=True
    )
    pkl_thread.start()
    
    # 启动Web服务器
    print("[WEB SERVER] Starting web_server.py...")
    from apps.crypto_screener.app import web_server  # noqa: E402
    web_server.main()


if __name__ == "__main__":
    main()
