# 导入必要的模块
import subprocess
import warnings
import sys
from pathlib import Path
warnings.filterwarnings("ignore")


def run_script(script_name):
    script_directory = Path("utils")  # 请替换为你的脚本所在的实际目录
    script_path = script_directory / script_name  # 使用pathlib构建完整路径
    return subprocess.Popen([sys.executable, str(script_path)])


if __name__ == '__main__':
    # 并行运行两个脚本
    process1 = run_script("合并币安合约数据.py")
    process2 = run_script("合并币安现货数据.py")

    # 等待两个进程都完成
    process1.wait()
    process2.wait()

    print("两个脚本都已完成执行。")
