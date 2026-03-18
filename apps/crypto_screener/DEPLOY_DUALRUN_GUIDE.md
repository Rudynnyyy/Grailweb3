# 双跑改造部署指南

## 1. 部署前检查

- 确认已合并以下文件：
  - `apps/crypto_screener/app/web_server.py`
  - `apps/crypto_screener/web/index.html`
  - `apps/crypto_screener/web/style.css`
  - `apps/crypto_screener/web/app.js`
  - `tests/test_dualrun_config.py`
  - `linux_deploy/smoke_dualrun_api.py`
- 确认静态资源版本参数已升级（避免浏览器缓存旧 JS/CSS）。

## 2. 关键环境变量

- `QC_DUALRUN_ENABLED`：是否启用双跑（`0/1`）
- `QC_ENGINE_PRIMARY`：主链路（`legacy/coin`）
- `QC_DUALRUN_SAMPLE_RATIO`：采样率（`0~1`）
- `QC_DUALRUN_MAX_SYMBOLS`：双跑请求最大 symbol 数
- `QC_DUALRUN_SPLIT_RULE`：切流规则（`user_hash/market_symbol`）

可选（已有）：
- `QC_PKL_CACHE_ROOT`
- `QC_PKL_REQUIRE_FRESH`

## 3. 上线步骤

1) 代码部署到目标机器  
2) 启动前执行语法与测试

```bash
python -m py_compile apps/crypto_screener/app/web_server.py
python -m unittest tests/test_dualrun_config.py
```

3) 启动服务并确认可访问

```bash
python apps/crypto_screener/app/web_server.py
```

4) 运行 dualrun API 冒烟

```bash
python linux_deploy/smoke_dualrun_api.py --base-url http://127.0.0.1:8001
```

5) 浏览器强刷（Ctrl+F5）验证“数据 > 双跑配置”面板可见，并可保存/刷新状态。

## 4. 灰度建议

- 阶段 1：`QC_DUALRUN_ENABLED=1`，`QC_ENGINE_PRIMARY=legacy`，`QC_DUALRUN_SAMPLE_RATIO=0.05`
- 阶段 2：采样率提升到 `0.1~0.2`，观察 drift 与 p95
- 阶段 3：按策略文档逐步切主链路到 `coin`

## 5. 回滚步骤

1) 将主链路切回 legacy：

```bash
QC_ENGINE_PRIMARY=legacy
QC_DUALRUN_ENABLED=0
```

2) 重启服务进程  
3) 清理前端缓存（保留版本参数可避免多数缓存问题）  
4) 验证 `/api/dualrun_config` 与 `/api/dualrun_metrics` 可读，`/api/latest_enriched` 正常返回

## 6. 验收口径

- 功能：双跑面板可读写配置，状态可刷新
- 稳定性：主流程（刷新、筛选、因子计算）无回归
- 性能：双跑开启后页面可用，busy 重试可恢复
- 测试：单测与 smoke 均通过

