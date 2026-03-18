# 导入必要的模块
import subprocess
import warnings
import sys
from pathlib import Path
warnings.filterwarnings("ignore")


def run_script(script_name):
    script_directory = Path(__file__).resolve().parent / "utils"  # 使用绝对路径，兼容PM2等任意cwd
    script_path = script_directory / script_name  # 使用pathlib构建完整路径
    return subprocess.Popen([sys.executable, str(script_path)])


if __name__ == '__main__':
    # 并行运行两个脚本
    process1 = run_script("合并币安合约数据.py")
    process2 = run_script("合并币安现货数据.py")

    # 等待两个进程都完成
    process1.wait()
    process2.wait()

    if int(process1.returncode or 0) != 0 or int(process2.returncode or 0) != 0:
        raise SystemExit(int(process1.returncode or process2.returncode or 1))

    print("两个脚本都已完成执行。")
