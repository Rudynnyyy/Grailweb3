# Route1 技术路线图（基于两份性能报告对齐）

本文对比两份文档：
- [route1_性能优化报告.md](file:///d:/量化交易/中性1.8/docs/route1_性能优化报告.md)
- [route1_性能优化报告_v2.md](file:///d:/量化交易/中性1.8/docs/route1_性能优化报告_v2.md)

目标：提取所有已明确的性能瓶颈、优化目标、已验证策略；基于差异与最新结论形成可落地路线图，并规定每个优化点的验证口径与回滚规则。

## 1. 性能瓶颈清单（两文档合并去重）

### 1.1 数据与网络
- B1：快照过大导致首屏慢（历史上 35MB；已通过精简快照缓解）  
  关联实现：[screener.py](file:///d:/量化交易/中性1.8/apps/crypto_screener/app/screener.py)、[generate_snapshot.py](file:///d:/量化交易/中性1.8/apps/crypto_screener/app/generate_snapshot.py)
- B2：大 JSON 反复压缩/传输与浏览器解析成本高（已通过预压缩与 ETag/304 缓解）  
  关联实现：[web_server.py](file:///d:/量化交易/中性1.8/apps/crypto_screener/app/web_server.py)

### 1.2 后端热点路径
- B3：`/api/latest_enriched` 批量 enrich 计算时序列读取（CSV/预处理/PKL）与表达式/指标计算叠加，易超时、并发下错误率高  
  关联实现：[web_server.py](file:///d:/量化交易/中性1.8/apps/crypto_screener/app/web_server.py)、[series_source.py](file:///d:/量化交易/中性1.8/apps/crypto_screener/app/series_source.py)、[expr_lang.py](file:///d:/量化交易/中性1.8/apps/crypto_screener/app/expr_lang.py)
- B4：更新链路“刷新等待过久”：前端刷新后等待后端更新结束，导致主页在预处理/PKL 构建期间不可用  
  关联实现：前端等待 [app.js](file:///d:/量化交易/中性1.8/apps/crypto_screener/web/app.js) 的 `waitBackendDone()`；后端更新 [web_server.py](file:///d:/量化交易/中性1.8/apps/crypto_screener/app/web_server.py) 的 `_run_update()`
- B5：企业微信选币“无命中”：筛选需要序列数据，但推送路径未必补齐 `series`，导致 MA/RSI 等条件无法计算而全量过滤  
  关联实现：`_config_needs_series()` 与 `_attach_series_to_rows()` 在 [web_server.py](file:///d:/量化交易/中性1.8/apps/crypto_screener/app/web_server.py)，筛选逻辑在 [filter_engine.py](file:///d:/量化交易/中性1.8/apps/crypto_screener/app/filter_engine.py)

### 1.3 数据构建与缓存
- B6：PKL 缓存构建耗时长（用户反馈 8min），瓶颈在“每小时大量 CSV 文本解析 + 指标计算 + 序列化”  
  关联实现：[factor_cache_update.py](file:///d:/量化交易/中性1.8/数据获取/factor_cache_update.py)

### 1.4 前端体验
- B7：点击计算因子/筛选后 UI 卡顿：前端同步处理大量行或频繁重渲染  
  关联实现：[app.js](file:///d:/量化交易/中性1.8/apps/crypto_screener/web/app.js)

## 2. 优化目标（两文档合并）

### 2.1 Route1 原始目标（报告.md）
- G1：热点从“客户端大快照计算”迁移到“服务端缓存计算 + 精简快照”，提升 100+ 并发下稳定性。
- G2：主页刷新与 `/api/kline` 作为热点路径，要求 RT 降低、错误率降低。

### 2.2 Route1 V2 增补目标（报告_v2.md）
- G3：CSV 更新完成后立即更新主页（不等待小时预处理与 PKL 构建）。
- G4：企业微信每小时按用户选择的因子/条件发送正确选币结果（不出现“无选中”误报）。
- G5：CSV→PKL 构建耗时显著降低（目标：分钟级以内，并且可持续演进到 Parquet/预处理格式直接消费）。
- G6：点击计算因子后界面不假死，保持可交互（分片/让出主线程）。

## 3. 已验证且已落地的优化策略（来自报告.md）

- S1：精简快照（移除全量 series），将快照体积从 ~35MB 降到通常 <3MB。  
  代码位置：[screener.py](file:///d:/量化交易/中性1.8/apps/crypto_screener/app/screener.py)、[generate_snapshot.py](file:///d:/量化交易/中性1.8/apps/crypto_screener/app/generate_snapshot.py)
- S2：快照预压缩 `latest.json.gz` + ETag/304（静态资源与 JSON）。  
  代码位置：[web_server.py](file:///d:/量化交易/中性1.8/apps/crypto_screener/app/web_server.py)
- S3：自定义因子后移到服务端：`/api/latest_enriched` 计算 `_expr`，前端只展示/筛选。  
  代码位置：[expr_lang.py](file:///d:/量化交易/中性1.8/apps/crypto_screener/app/expr_lang.py)、[web_server.py](file:///d:/量化交易/中性1.8/apps/crypto_screener/app/web_server.py)
- S4：自定义因子持久化（SQLite）保证跨设备一致。  
  代码位置：[auth_sqlite.py](file:///d:/量化交易/中性1.8/apps/db/auth_sqlite.py)、[app.js](file:///d:/量化交易/中性1.8/apps/crypto_screener/web/app.js)
- S5：可观测性与缓存：latest JSON 缓存、表达式 AST 缓存、K 线缓存、enriched 聚合缓存、`/api/metrics` 输出。  
  代码位置：[web_server.py](file:///d:/量化交易/中性1.8/apps/crypto_screener/app/web_server.py)

## 4. 差异点与最新结论（报告_v2.md 相对报告.md）

- D1：报告.md 侧重“线上性能”与“服务端计算迁移”；报告_v2.md 增补“更新链路解耦、企微可靠性、PKL 构建耗时、前端卡顿”四个交付型问题。
- D2：导致“主页等待”的根因不是快照生成，而是前端等待后端“整个更新任务结束”，而后端把预处理放在同一 running 区间内。
- D3：企微“无命中”的根因偏向“序列未附加/条件需要序列但未触发补齐”，属于确定性逻辑缺陷，可通过单点修复解决。
- D4：PKL 构建耗时需要两条线并行推进：短期减少每小时无意义重算（内容不变但 mtime 变动），中期消费预处理产物（pkl/parquet）替代 CSV 文本解析。

## 5. 可落地技术路线图（优先级 / 依赖 / 里程碑）

### 5.1 总体原则
- 每个优化点只改一个单点问题，配套：单元测试 + 集成测试 + 性能回归测试。
- 每次优化完成后，在同一压测模型下重跑，要求：RT 下降或 QPS 提升 ≥10%，且 CPU/内存不劣化；否则回滚并复盘。

### 5.2 里程碑（M0~M4）

#### M0（基线与工具链，P0）
- 交付物：
  - 可在主干环境复现的压测脚本（优先 k6，其次 Python 压测器）。
  - 五指标采集：CPU、内存、RT、QPS、错误率，落盘为 JSON。
  - 可选：仅用于本地压测的匿名访问开关（默认关闭）。
- 依赖：Web 服务可启动；压测脚本可访问 `/api/latest_enriched`。

#### M1（更新链路解耦：主页先更新，P0）
- 目标：前端刷新只等待“快照就绪”，不等待预处理/PKL。
- 改动点：
  - 后端把预处理与 PKL 构建迁移到后台线程。
  - `/api/status` 新增 preprocess/pkl 独立状态。
- 依赖：M0 的基线脚本与五指标采集。

#### M2（企微选币可靠性修复，P0）
- 目标：企业微信每小时选币不再“无命中误报”。
- 改动点：
  - 修复 `_config_needs_series()`：当开启 `condCloseMa/condMa/condRsi` 时必须补齐 `series`。
  - 增强推送路径的最小自检：`series` 附加比例、空结果原因可见。
- 依赖：M1（保证更新链路不阻塞推送时序），以及 `/api/kline` 数据源可用。

#### M3（PKL 构建耗时下降，P1）
- 目标：将“每小时 CSV→PKL”耗时压缩到分钟级，并为后续数据格式升级铺路。
- 改动点：
  - `factor_cache_update.py` 增量策略优化：当内容未新增时跳过解析与计算（即使 mtime 变化也不全量重算）。
  - 可选：消费预处理产物（pkl/parquet）替代 CSV 文本解析。
- 依赖：M0（基线）；M1（后台化避免阻塞业务）；M2（推送先正确）。

#### M4（前端交互顺滑，P1）
- 目标：点击计算因子不假死、滚动/交互保持流畅。
- 改动点：
  - 前端将 CPU 密集循环改为分片执行，使用 `requestIdleCallback`/短延时让出主线程。
- 依赖：M1/M2/M3 均可独立于前端改动并行推进，但回归压测需沿用 M0 口径。

## 6. 压测与回滚口径（用于后续实施阶段）

- 压测模型：固定 endpoint=`/api/latest_enriched`，固定 payload，固定并发与持续时间；每次变更只改一个点并复测。
- 通过阈值：RT（p95）下降或 QPS 提升 ≥10%，且错误率不升、CPU/内存不劣化。
- 回滚动作：撤销本次变更并重跑基线，记录复盘结论与下一步假设。

