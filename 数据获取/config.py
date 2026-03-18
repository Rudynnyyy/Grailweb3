
import time
import warnings
from datetime import datetime, timedelta
import os
import shutil
import pandas as pd
warnings.filterwarnings('ignore')

root_path = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径
print('当前文件路径', root_path)
# 获取目录位置，不存在就创建目录
data_center_path = os.path.join(root_path, 'data', 'data_center')
if not os.path.exists(data_center_path):
    try:
        os.makedirs(data_center_path)
    except FileExistsError:
        pass
# 修复整个data目录可能由root创建的权限问题（确保当前用户可写）
try:
    _data_root = os.path.join(root_path, 'data')
    for _dirpath, _dirnames, _filenames in os.walk(_data_root):
        try:
            os.chmod(_dirpath, 0o755)
        except Exception:
            pass
        for _fname in _filenames:
            try:
                os.chmod(os.path.join(_dirpath, _fname), 0o644)
            except Exception:
                pass
except Exception:
    pass

runtime_mode = 'close'  # 设置获取K线模式，close表示获取过去一小时关闭的K线，curret为当前整点到此刻的K线。自行选择current，或者close,个人建议使用close
server_swap_path = os.path.join(data_center_path, 'kline', 'swap')
if not os.path.exists(server_swap_path):
    try:
        os.makedirs(server_swap_path)
    except FileExistsError:
        pass
else:
    # 修复可能由root创建的目录权限问题
    try:
        os.chmod(server_swap_path, 0o755)
    except Exception:
        pass
if os.name == "nt":
    _default_swap = r'D:\量化交易\数据\swap_lin'
    _default_spot = r'D:\量化交易\数据\spot_lin'
else:
    _default_swap = os.path.join(root_path, 'data', 'swap_lin')
    _default_spot = os.path.join(root_path, 'data', 'spot_lin')

local_swap_path = os.environ.get("QC_LOCAL_SWAP_PATH") or _default_swap
merge_swap_path = os.environ.get("QC_MERGE_SWAP_PATH") or local_swap_path
local_spot_path = os.environ.get("QC_LOCAL_SPOT_PATH") or _default_spot
merge_spot_path = os.environ.get("QC_MERGE_SPOT_PATH") or local_spot_path
proxy_http = (os.environ.get("QC_HTTP_PROXY") or "").strip()
proxy_https = (os.environ.get("QC_HTTPS_PROXY") or proxy_http).strip()
proxy = {'http': proxy_http, 'https': proxy_https} if (proxy_http or proxy_https) else None  # 没有就是None


if not os.path.exists(merge_swap_path):
    try:
        os.makedirs(merge_swap_path)
    except FileExistsError:
        pass
server_spot_path = os.path.join(data_center_path, 'kline', 'spot')
if not os.path.exists(server_spot_path):
    try:
        os.makedirs(server_spot_path)
    except FileExistsError:
        pass
else:
    # 修复可能由root创建的目录权限问题
    try:
        os.chmod(server_spot_path, 0o755)
    except Exception:
        pass

if not os.path.exists(merge_spot_path):
    try:
        os.makedirs(merge_spot_path)
    except FileExistsError:
        pass

only_last_hours = int(os.environ.get("QC_ONLY_LAST_HOURS") or "1")
only_last_hours = max(1, only_last_hours)
max_backfill_hours = int(os.environ.get("QC_MAX_BACKFILL_HOURS") or "168")
max_backfill_hours = max(1, max_backfill_hours)
kline_num = max(3, only_last_hours + 2)  # 少量缓冲，避免边界时间
now_utc0_hour = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
print("当前UTC+0时间:", now_utc0_hour)
print("仅获取最近小时数", only_last_hours, "本次下载K线数量", kline_num)
utc_offset = 8
print('本地时间与UTC相差' + str(utc_offset) + '小时')

stable_symbol = ['BKRW', 'USDC', 'USDP', 'TUSD', 'BUSD', 'FDUSD', 'DAI', 'EUR', 'GBP']

# =====特殊现货对应列表
special_symbol_dict = {
    'DODO': 'DODOX',  # DODO现货对应DODOX合约
    'LUNA': 'LUNA2',  # LUNA现货对应LUNA2合约
    '1000SATS': '1000SATS',  # 1000SATS现货对应1000SATS合约
}

exchange_basic_config = {
    'timeout': 30000,
    'rateLimit': 200,
    'enableRateLimit': True,
    'options': {
        'adjustForTimeDifference': True,
        'recvWindow': 10000,
    },
    'proxies': proxy
}


# =====本地电脑与服务器的时差
class GlobalVariable:
    diff_timestamp = 0

    def update_diff_time(self, _t):
        self.diff_timestamp = _t


# 存储这个全局变量
glob_var = GlobalVariable()
