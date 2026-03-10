# Linux 部署快速启动

## 需要上传到服务器的目录/文件
- `apps/`
- `数据获取/`
- `linux_deploy/`
- `requirements.txt`（如可用）

## 启动网页（包含数据抓取+每小时更新）
在仓库根目录执行：

```bash
chmod +x linux_deploy/*.sh
./linux_deploy/run_web.sh
```

默认监听 `0.0.0.0:8001`，浏览器访问：

`http://<服务器IP>:8001/`

## 只跑一次数据更新（不启动网页）

```bash
chmod +x linux_deploy/*.sh
./linux_deploy/run_update_once.sh
```

## 环境变量（可选）
把 `linux_deploy/env.example` 复制成你自己的 `.env`，并在 shell 中 `export` 对应变量，或写进 systemd。

默认情况下，Linux 会把合并后的历史数据写到 `数据获取/data/{swap_lin,spot_lin}`，K线接口与因子计算也会从这里读取。

## 每小时 05 分增量预处理

```bash
crontab -e
```

```bash
5 * * * * cd /path/to/repo && /usr/bin/python3 数据获取/incremental_update.py --config 数据获取/config.yaml --once >> 数据获取/logs/cron_incremental.log 2>&1
```
