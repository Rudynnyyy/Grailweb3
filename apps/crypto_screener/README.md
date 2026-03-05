# 币圈选股器（隔离版）

本目录为独立功能区，不依赖主程序的回测/选币流程；数据优先从“数据获取”脚本输出的 data_center 读取并生成网页快照。

## 目录结构
- apps/crypto_screener/app：Python 逻辑（计算因子、筛选、生成 JSON）
- apps/crypto_screener/web：静态网页（读取 JSON 展示）
- apps/crypto_screener/web/data：输出快照（meta.json / latest.json）

## 生成快照（读取 data_center）

```powershell
C:/anaconda3/envs/Gamma/python.exe apps/crypto_screener/app/generate_snapshot.py
```

如果需要指定输出目录：

```powershell
C:/anaconda3/envs/Gamma/python.exe apps/crypto_screener/app/generate_snapshot.py --out-dir apps/crypto_screener/web/data
```

## 每小时自动运行（两种方式）
### 方式A：脚本常驻

```powershell
C:/anaconda3/envs/Gamma/python.exe apps/crypto_screener/app/run_hourly.py --mode forever
```

### 方式B：Windows 任务计划程序
- 每小时触发一次，执行：

```powershell
C:/anaconda3/envs/Gamma/python.exe apps/crypto_screener/app/run_hourly.py --mode once
```

## 打开网页
启动静态服务：

```powershell
cd apps/crypto_screener/web
C:/anaconda3/envs/Gamma/python.exe -m http.server 8001
```

浏览器打开：
- http://localhost:8001/

