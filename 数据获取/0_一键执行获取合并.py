import subprocess
import sys
from pathlib import Path

# 始终使用本文件所在目录作为基准，兼容PM2等任意cwd
_HERE = Path(__file__).resolve().parent

# 执行A.py
print("开始获取Kline")
subprocess.run([sys.executable, str(_HERE / "1_kline_update.py")], cwd=str(_HERE), check=True)
print("获取Kline完毕")

# 执行B.py
print("开始Kline合并")
subprocess.run([sys.executable, str(_HERE / "2_币安数据合并.py")], cwd=str(_HERE), check=True)
print("Kline合并完毕")
