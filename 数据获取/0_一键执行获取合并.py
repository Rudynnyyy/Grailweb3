import subprocess
import sys

# 执行A.py
print("开始获取Kline")
subprocess.run([sys.executable, "1_kline_update.py"])
print("获取Kline完毕")

# 执行B.py
print("开始Kline合并")
subprocess.run([sys.executable, "2_币安数据合并.py"])
print("Kline合并完毕")
print("开始小时分区预处理")
subprocess.run([sys.executable, "incremental_update.py", "--config", "config.yaml", "--once"])
print("小时分区预处理完毕")
