from __future__ import annotations

import os
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


@dataclass(frozen=True)
class PipelinePaths:
    repo_root: Path
    gamma_python: str
    snapshot_script: Path
    snapshot_out_dir: Path
    lock_file: Path


class PipelineLock:
    def __init__(self, lock_file: Path):
        self.lock_file = lock_file
        self.fd: int | None = None

    def _pid_alive(self, pid: int) -> bool:
        if pid <= 0:
            return False
        if os.name == "nt":
            try:
                import ctypes
                from ctypes import wintypes

                PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
                STILL_ACTIVE = 259
                handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
                if not handle:
                    return False
                code = wintypes.DWORD()
                ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(code))
                ctypes.windll.kernel32.CloseHandle(handle)
                return int(code.value) == STILL_ACTIVE
            except Exception:
                return True
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except Exception:
            return True

    def acquire(self) -> None:
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_CREAT | os.O_EXCL | os.O_RDWR
        try:
            self.fd = os.open(str(self.lock_file), flags)
            os.write(self.fd, str(os.getpid()).encode("utf-8"))
        except FileExistsError:
            pid = None
            try:
                raw = self.lock_file.read_text(encoding="utf-8").strip()
                pid = int(raw) if raw else None
            except Exception:
                pid = None

            if pid is not None:
                if not self._pid_alive(pid):
                    try:
                        self.lock_file.unlink(missing_ok=True)
                        self.fd = os.open(str(self.lock_file), flags)
                        os.write(self.fd, str(os.getpid()).encode("utf-8"))
                        return
                    except Exception:
                        pass
            try:
                st = self.lock_file.stat()
                age = datetime.now() - datetime.fromtimestamp(st.st_mtime)
                if age > timedelta(hours=2):
                    self.lock_file.unlink(missing_ok=True)
                    self.fd = os.open(str(self.lock_file), flags)
                    os.write(self.fd, str(os.getpid()).encode("utf-8"))
                    return
            except Exception:
                pass
            raise RuntimeError(f"已有实例在运行：{self.lock_file}")

    def release(self) -> None:
        if self.fd is None:
            return
        try:
            os.close(self.fd)
        finally:
            self.fd = None
            try:
                self.lock_file.unlink(missing_ok=True)
            except Exception:
                pass

    def __enter__(self) -> "PipelineLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


def default_gamma_python() -> str:
    return os.environ.get("QC_GAMMA_PYTHON") or r"C:/anaconda3/envs/Gamma/python.exe"


def default_paths() -> PipelinePaths:
    repo_root = Path(__file__).resolve().parents[3]
    crypto_screener_dir = Path(__file__).resolve().parents[1]
    out_dir = crypto_screener_dir / "web" / "data"
    return PipelinePaths(
        repo_root=repo_root,
        gamma_python=default_gamma_python(),
        snapshot_script=crypto_screener_dir / "app" / "generate_snapshot.py",
        snapshot_out_dir=out_dir,
        lock_file=crypto_screener_dir / "web" / "data" / ".pipeline.lock",
    )


def run_python(python_exe: str, script: Path, cwd: Path, extra_args: list[str] | None = None) -> None:
    if not script.exists():
        raise FileNotFoundError(str(script))
    cmd = [python_exe, str(script)]
    if extra_args:
        cmd.extend(extra_args)
    subprocess.run(cmd, cwd=str(cwd), check=True)


def run_data_fetch(repo_root: Path, gamma_python: str) -> None:
    data_fetch_dir = repo_root / "数据获取"
    run_python(gamma_python, data_fetch_dir / "0_一键执行获取合并.py", data_fetch_dir)


def run_snapshot(paths: PipelinePaths) -> None:
    run_python(
        paths.gamma_python,
        paths.snapshot_script,
        paths.repo_root,
        extra_args=["--out-dir", str(paths.snapshot_out_dir)],
    )


def run_once(paths: PipelinePaths, *, fetch: bool = True) -> None:
    with PipelineLock(paths.lock_file):
        if fetch:
            run_data_fetch(paths.repo_root, paths.gamma_python)
        run_snapshot(paths)


def run_once_two_stage(paths: PipelinePaths, *, fetch: bool = True) -> None:
    """
    两阶段数据流：
    1. 获取CSV数据 + 生成快照（3-4分钟）
    2. 后台生成PKL预处理数据（8-9分钟）
    
    前端先展示CSV数据，PKL生成完成后自动切换
    """
    with PipelineLock(paths.lock_file):
        if fetch:
            run_data_fetch(paths.repo_root, paths.gamma_python)
        # 第一阶段：生成CSV快照，前端立即可用
        run_snapshot(paths)
        # 标记CSV快照已就绪
        csv_ready_marker = paths.snapshot_out_dir / ".csv_ready"
        csv_ready_marker.write_text(datetime.now(timezone.utc).isoformat())


def run_pkl_build_background(repo_root: Path, gamma_python: str) -> None:
    """后台生成PKL预处理数据"""
    try:
        data_fetch_dir = repo_root / "数据获取"
        run_python(
            gamma_python,
            data_fetch_dir / "factor_cache_update.py",
            data_fetch_dir,
            extra_args=["--market", "all", "--tail", "2160"]
        )
        # 标记PKL已就绪
        pkl_ready_marker = repo_root / "apps" / "crypto_screener" / "web" / "data" / ".pkl_ready"
        pkl_ready_marker.write_text(datetime.now(timezone.utc).isoformat())
    except Exception as e:
        import sys
        print(f"[PKL BUILD ERROR] {e}", file=sys.stderr)


def sleep_until_next_hour() -> None:
    now = datetime.now()
    next_hour = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    seconds = max(1.0, (next_hour - now).total_seconds())
    time.sleep(seconds)


def run_forever(paths: PipelinePaths, *, fetch: bool = True) -> None:
    """持续运行，支持两阶段数据流"""
    import threading
    
    while True:
        # 第一阶段：获取CSV数据并生成快照
        run_once_two_stage(paths, fetch=fetch)
        
        # 第二阶段：后台生成PKL数据（不阻塞主循环）
        pkl_thread = threading.Thread(
            target=run_pkl_build_background,
            args=(paths.repo_root, paths.gamma_python),
            daemon=True
        )
        pkl_thread.start()
        
        sleep_until_next_hour()
