# 双跑对照迁移蓝图（coin-realtime-data_v1.1.8 → crypto_screener）

> 目标：在不影响线上可用性的前提下，将 coin-realtime-data_v1.1.8 的“离线预处理/落盘产物”能力迁移到网站数据更新链路中，并通过“双跑对照”证明正确性与性能后再逐步切流。

## 0. 前置约束与合规

- 版权/使用约束：coin-realtime-data_v1.1.8 的 README 声明仅供个人使用、禁止传播；任何迁移代码与产物共享需确认合规边界并保留原项目许可条款。
- 运行环境约束：当前仓库 `requirements.txt` 仅声明 `xbx-py11`；coin 工程代码实际依赖 `pandas/aiohttp/requests/...`。迁移应避免把重依赖引入到 web_server 在线请求路径，优先放在“离线/更新任务”中。
- 在线 SLA 目标参考：`enriched P95≤4s, P99≤6s`（见 linux_deploy/optimization_validation.py）。

## 1. 现有系统摘要（对照基线）

### 1.1 coin-realtime-data_v1.1.8：计算与产物（离线批处理）

- 入口：coin-realtime-data_v1.1.8/realtime_data.py
- 流程：调度 → 并行抓取 → 重采样 → 预处理（market dict 分批 + 年度 pivot）→ `.ready` 就绪信号
- 核心落盘：
  - 单 symbol：`{trade_type}_{tf}/{symbol}.pkl`
  - ready：`{symbol}_{unix}.ready`
  - 预处理：`preprocess_1h_resample/{offset}/spot_dict_batch{n}.pkl`、`market_pivot_{trade_type}_{year}.pkl`

### 1.2 crypto_screener：更新与查询（在线按请求动态算）

- 更新入口：web_server.py 的 `_scheduler_loop` / `POST /api/refresh` → pipeline.run_once → generate_snapshot.py 产出 `web/data/latest.json(.gz)`
- 在线查询：`POST /api/latest_enriched` 会按请求动态拉序列 + 计算指标/表达式（web_server.py）
- 序列读取优先级：pkl_cache → preprocessed → csv（series_source.py）
- pkl_cache 格式：`QC_PKL_CACHE_ROOT/series_{market}.pkl`，结构：
  - `{"symbols": {"BTCUSDT": {"dt":[...], "series":{"open":[...],...}}}}`

## 2. 迁移总体策略（Dual-Run First）

迁移不以“直接替换在线计算”为目标，而是把 coin 工程的优势沉到网站的更新链路中：

1) **把 coin 的增量/重采样/预处理产物导入为 crypto_screener 可直接消费的 pkl_cache（series_{market}.pkl）**  
2) 在线 `/api/latest_enriched` 仍按现有逻辑运行，但序列读取会优先命中 pkl_cache，从而显著降低 IO 与尾延迟  
3) 引入“双跑对照”评测：同一请求集同时跑 legacy 与 coin-backed 两条链路，输出差异报告与性能报告

## 3. 双跑对照的设计

### 3.1 双跑范围定义（最小可行切片）

- 双跑对象：`/api/latest_enriched` 的核心输出字段
  - `_builtins`：如 ma/rsi/ema/boll/super/kdj/obv/stoch_rsi
  - `_expr`：自定义表达式输出
  - `results` 的基础字段（close/pct_change 等）由 latest.json 决定，不作为迁移差异主责
- 双跑粒度：按“symbol 子集”与“字段子集”逐步扩大
  - 子集：100 → 500 → 2000（现货/合约分别覆盖）
  - 字段：先 builtins，再 expr，再 include_debug（如需要）

### 3.2 双跑模式（推荐实现）

在服务端增加一个“内部对照执行器”，不改变对外接口语义：

- 新增环境开关：
  - `QC_DUALRUN_ENABLED=1|0`
  - `QC_DUALRUN_SAMPLE_RATIO=0.0~1.0`（随机采样请求）
  - `QC_DUALRUN_MAX_SYMBOLS=...`
  - `QC_ENGINE_PRIMARY=legacy|coin`
- 双跑执行逻辑：
  - primary：正常返回给用户（legacy 或 coin）
  - shadow：后台跑另一条链路，仅记录差异与耗时，不影响响应

实现方式二选一：

1) **双读取路径（低侵入）**  
   - legacy：保持现有 `series_source` 读取优先级（pkl_cache/preprocessed/csv）
   - coin：强制只读 pkl_cache（新增 `QC_FORCE_PKL_CACHE=1` 或在调用处传参）
   - 优点：不改指标计算逻辑，只验证“数据源迁移”是否导致漂移；最快落地
2) **双计算引擎（高收益）**  
   - legacy：现有 enriched 计算
   - coin：引入 precomputed lookup（把部分指标/表达式提前预计算落盘）
   - 优点：在线更快；缺点：需要完整对齐表达式语义与内置指标实现

本蓝图优先落地“**双读取路径**”，先把最不确定的变量（数据源/重采样/缺失值）对齐。

### 3.3 差异度量（必须可量化）

- 数值误差：
  - `abs_err = |a-b|`
  - `rel_err = abs_err / max(|b|, eps)`，eps=1e-12
- 判定阈值：
  - **准确性**：`rel_err ≤ 0.0001`（0.01%）或 `abs_err ≤ min_abs`（例如 1e-8）
- 排序漂移：
  - 对 topN（如 200）按某 sort_key 计算 Spearman/Kendall 或“位置变动绝对值均值”
  - 设定门槛：topN 一致率≥99.5%，或平均名次漂移≤1
- 覆盖率：
  - legacy 有值但 coin 为 null 的比例（反之亦然）
  - 设定门槛：null 漂移≤0.5%

### 3.4 报告产物（机器可读 + 人可读）

- 机器可读（用于自动化门禁）：
  - `dualrun_summary.json`：请求数、差异率、P50/P95/P99 耗时、错误码分布、资源峰值
  - `dualrun_drift_top.jsonl`：按 symbol+field 记录 top 漂移样本（前 500/1000 条）
- 人可读：
  - `dualrun_report.md`：结论、漂移原因归类、建议

建议落盘路径：
- `apps/crypto_screener/app/perf_outputs/dualrun/`（与现有 perf_outputs 对齐）

## 4. 将 coin 产物导入网站更新链路（数据面迁移）

### 4.1 目标：生成 crypto_screener 可直接消费的 pkl_cache

目标路径（可配置）：
- `QC_PKL_CACHE_ROOT` 指向：`.../preprocessed_hourly/pkl_cache`
- 生成文件：
  - `series_spot.pkl`
  - `series_swap.pkl`

文件结构必须符合 series_source.py 的读取格式：

```json
{
  "symbols": {
    "BTCUSDT": {
      "dt": ["2026-03-10T10:00:00+00:00", "..."],
      "series": {
        "open": [..],
        "high": [..],
        "low": [..],
        "close": [..],
        "volume": [..],
        "quote_volume": [..]
      }
    }
  }
}
```

### 4.2 导入来源（两条可选）

1) **直接读取 coin 的 per-symbol pkl 文件**（最贴近源系统）
   - 需要明确 timeframe 与 offset（建议与网站 1h 最新对齐）
   - 需要定义 symbol 命名规范（`BTCUSDT` vs `BTC-USDT`）并做映射
2) **读取 coin 的 preprocess market_dict_batch**（吞吐更高）
   - market_dict_batch 本身是 dict[symbol]->DataFrame，适合批量转换为 series_{market}.pkl
   - 需要依赖 pandas（仅在更新任务中）

推荐：优先 2），因为它已经把源数据规整为批结构，减少文件遍历与随机 IO。

### 4.3 接入点

在 crypto_screener 的更新链路 run_once(fetch) 中，增加可选阶段：

- 阶段名：`coin_ingest_to_pkl_cache`
- 触发开关：`QC_ENABLE_COIN_INGEST=1`
- 运行时机：
  - 在 generate_snapshot 之前：保证 snapshot 与 pkl_cache 同步到同一时点
  - 或在 generate_snapshot 之后：只影响后续在线 enriched（更安全但短暂不一致）

建议采用“两阶段提交”避免半成品：

1) 写入临时文件：`series_spot.pkl.tmp` / `series_swap.pkl.tmp`
2) 校验：dt 单调、长度≥tail_min、字段齐全
3) 原子替换：rename → `series_spot.pkl` / `series_swap.pkl`

## 5. 并发与隔离（必须解决 busy/排队问题）

### 5.1 资源隔离建议

- 将 coin_ingest 与 pkl_build 放到“后台更新线程”中，并限制并发：
  - 同一时刻只允许一个重任务（更新/导入/pkl build）运行
  - 与在线查询的 CPU/IO 做隔离：建议至少做到“导入期减少在线 QPS”，或反代分流

### 5.2 降级策略

- 若 coin_ingest 失败：
  - 不更新 pkl_cache（保留上一版）
  - 在线查询继续回退到 legacy（preprocessed/csv）
- 若 pkl_cache 不新鲜：
  - `QC_PKL_REQUIRE_FRESH=1` 时会自动判 stale 并回退（series_source.py 已实现）

## 6. 双跑测试计划（可直接执行）

### 6.1 数据集与请求集固定化

- 固定 symbol 列表：spot 250 + swap 250（覆盖大市值+长尾+新币）
- 固定 tail：360/720 两档
- 固定 toggles/params：覆盖至少 3 套典型策略（例如：仅均线/均线+RSI/全开）
- 固定自定义因子：至少 20 个表达式，包含极值/缺失/分母为 0 等边界

### 6.2 离线对账（不经 HTTP）

目的：先把“同输入→同输出”的正确性跑通。

- 跑法：写一个脚本（建议放 linux_deploy/dualrun_offline.py）：
  - 读取同一份 latest.json
  - 对同一批 symbols 分别调用 legacy 与 coin-only 读取路径
  - 输出差异统计与 top 漂移样本

门禁：
- rel_err>0.01% 的样本占比 ≤0.5%
- top200 一致率 ≥99.5%

### 6.3 在线对账（经 HTTP，含并发）

目的：验证真实链路 SLA 与资源峰值。

- 单用户：连续 30 次请求，记录 P50/P95/P99
- 10 用户：并发压测 5 分钟，记录错误率与尾延迟
- 100 用户：只在灰度环境跑，重点看 429/504/5xx

门禁：
- primary 路径的 P95/P99 不劣于当前线上基线
- busy/429 比例在可控阈值内（建议 ≤1%）

## 7. 灰度切流方案（与双跑衔接）

阶段 0：仅导入 pkl_cache，不改 primary（验证数据面）

阶段 1：双跑对照（shadow 记录差异），primary 仍 legacy

阶段 2：逐步切流 primary 到 coin（按用户/比例）
- 1% → 5% → 20% → 50% → 100%
- 任一阶段触发门禁（漂移/延迟/错误率）立即自动回滚到 legacy

## 8. 监控与告警（上线必备）

必须新增或补齐的指标：

- 请求侧：
  - `/api/latest_enriched`：count、p50/p95/p99、5xx、429、504
  - busy 重试次数分布（前端/后端各自）
- 双跑侧：
  - drift_ratio（超阈值样本/总样本）
  - null_drift_ratio
  - topN_rank_diff
  - shadow_cost_ms（shadow 额外耗时）
- 资源侧：
  - 进程 CPU/RSS、磁盘 IO、网络吞吐（可复用现有 bench 采集口径）

告警建议：
- drift_ratio > 0.5% 持续 5 分钟
- enriched p99 > 目标阈值持续 5 分钟
- 429 比例突增（较 1h 均值 + 3σ）

## 9. 验收标准（最终切换门禁）

### 9.1 正确性

- rel_err ≤0.01% 的覆盖率 ≥99.5%
- top200 排序一致率 ≥99.5%
- null 漂移率 ≤0.5%

### 9.2 性能

- enriched P95 ≤ 4s，P99 ≤ 6s（同样请求集+同并发模型）
- 504 比例 ≤0.5%
- pkl 构建/导入与在线查询共存时，线上 P99 不劣化超过 10%

### 9.3 可回滚

- 配置开关 1 分钟内生效
- 回滚后 10 分钟内指标恢复到基线范围

## 10. 实施清单（按任务拆分）

### A. 数据面（coin → pkl_cache）

- [ ] 定义 symbol 映射与 market 定义（spot/swap）
- [ ] 实现 coin_ingest_to_pkl_cache（含两阶段提交）
- [ ] 增加 fresh 校验（dt 最新值与 latest.json 对齐策略）
- [ ] 将导入阶段纳入更新链路（QC_ENABLE_COIN_INGEST）

### B. 双跑执行器与报告

- [ ] 实现 coin-only 读取路径开关（强制只读 pkl_cache）
- [ ] 实现 shadow 执行（不影响 primary 响应）
- [ ] 实现 drift 统计与落盘（json + jsonl）
- [ ] 增加门禁脚本（离线/在线）

### C. 灰度与监控

- [ ] 增加 QC_ENGINE_PRIMARY、QC_DUALRUN_SAMPLE_RATIO
- [ ] 增加关键 metrics 与告警阈值
- [ ] 编写回滚 SOP（含反代/环境变量/服务重启步骤）

