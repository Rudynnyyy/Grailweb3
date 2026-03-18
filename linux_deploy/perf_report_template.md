# Crypto Screener 性能基准报告模板

## 1. 基本信息

| 项目 | 内容 |
| --- | --- |
| 报告日期 | YYYY-MM-DD |
| 报告人 |  |
| 版本/分支 |  |
| 环境 | 生产同规格 / 预发 |
| 数据窗口 |  |
| 测试机配置 | CPU / 内存 / 磁盘 |

## 2. 变更范围

- 本次优化项：
  - [ ] 缓存淘汰策略（LRU+TTL）
  - [ ] latest_enriched 字段裁剪
  - [ ] 前端分帧渲染
  - [ ] 调度策略统一
  - [ ] 安全配置修订

## 3. 测试场景

| Case | 描述 | 请求参数 | 样本量 |
| --- | --- | --- | --- |
| A | 仅快照渲染 | 无动态指标 |  |
| B | StochRSI 开启 | stoch=on |  |
| C | StochRSI + 3 因子 | factor=3 |  |
| D | 市场 all + 手动刷新 | market=all |  |

## 4. 核心指标对比

| 指标 | 基线值 | 优化后 | 变化率 | 目标阈值 | 是否达标 |
| --- | --- | --- | --- | --- | --- |
| 首屏可见 P95 |  |  |  | ≤1.2s |  |
| enriched P95 |  |  |  | ≤4.0s |  |
| enriched P99 |  |  |  | ≤6.0s |  |
| 504 比例 |  |  |  | <0.5% |  |
| web_server CPU 峰值 |  |  |  | 降低≥30% |  |
| 进程内存峰值 |  |  |  | 降低≥15% |  |
| 单次响应 payload |  |  |  | 降低≥20% |  |

## 5. 证据附件

- 自动化验证输出：
  - `linux_deploy/reports/optimization_validation_*.json`
- 压测原始文件：
  - `apps/crypto_screener/app/perf_outputs/http_bench_small.json`
  - `apps/crypto_screener/app/perf_outputs/hotspots.json`
- 诊断脚本输出：
  - `linux_deploy/diag_data_flow.py`
  - `linux_deploy/diag_latest_json.py`

## 6. 问题与风险

| 问题 | 影响 | 临时缓解 | 长期方案 | 负责人 |
| --- | --- | --- | --- | --- |
|  |  |  |  |  |

## 7. 发布建议

- [ ] 建议全量发布
- [ ] 建议灰度继续观察
- [ ] 建议回滚

结论说明：
