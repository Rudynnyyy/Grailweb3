# 网页升级改造报告（基于 MIGRATION_BLUEPRINT_DUALRUN）

## 1. 改造目标

- 将双跑治理能力前置到网页运维面板，支持在线查看与配置。
- 在不破坏现有筛选/因子流程的前提下，补齐响应式与可访问性基础能力。
- 对齐技术路线文档中的关键约束：灰度开关、采样配置、可观测指标展示、可回滚操作路径。

## 2. 架构与模块改造

### 前端结构（HTML）

- 在数据区块新增“双跑配置”入口与折叠面板：
  - 双跑启用、主链路、采样率、max_symbols、切流规则
  - 保存配置、刷新状态
  - 双跑状态与指标摘要
- 为主布局与关键区域补充语义化属性：
  - `role="application"`、`aria-label`
  - 因子进度条增加 `role="progressbar"` 与 `aria-valuenow`

### 样式体系（CSS）

- 新增双跑面板布局样式：
  - `dualrun-action-row`
  - `dualrun-grid`
- 补齐键盘可见焦点样式：
  - `button/a/summary:focus-visible`
- 新增两档响应式断点：
  - `max-width:1200px`：放宽滚动策略与布局高度
  - `max-width:960px`：侧栏下沉、内容区增高、表格字体压缩

### 功能模块（JavaScript）

- 新增双跑配置草稿管理（localStorage）：
  - 读取、回填、保存草稿
- 新增双跑 API 交互：
  - `GET /api/dualrun_config`
  - `POST /api/dualrun_config`
  - `GET /api/dualrun_metrics`
- 新增双跑状态渲染：
  - 主链路/采样率/max_symbols/total/shadow/drift/p95/qps/更新时间
- 在刷新链路中联动双跑指标拉取（启用时自动刷新）。
- 因子进度条同步更新 `aria-valuenow`。

### 后端能力（web_server）

- 新增配置与指标内存状态：
  - `dualrun_config`
  - `dualrun_metrics`
- 新增配置清洗函数：
  - `_sanitize_dualrun_config`
- 新增请求观测函数：
  - `_dualrun_record_request`
  - 基于 `/api/latest_enriched` 请求实时统计 total、error、p95、qps、shadow_runs
- 新增 API：
  - `GET /api/dualrun_config`
  - `POST /api/dualrun_config`
  - `GET /api/dualrun_metrics`

## 3. 一致性与兼容性说明

- 本次改造未变更 `/api/latest_enriched` 输出 schema，不影响既有表格渲染与策略筛选流程。
- 双跑配置默认走“保守值”并支持本地草稿回退，后端不可用时前端仍可正常使用主功能。
- 旧浏览器降级行为：即使不支持 `:focus-visible`，仍不影响主要交互；响应式布局在窄屏下可读性显著提升。

## 4. 性能优化点

- 双跑指标刷新按需触发（仅启用双跑时联动请求）。
- 面板交互采用轻量 DOM 更新，不引入额外库。
- 保持原有分块 enriched 请求机制，并继续支持 busy 重试与动态 chunk。

## 5. 测试与验收

### 单元测试

- 新增：`tests/test_dualrun_config.py`
  - 双跑配置清洗逻辑边界验证
  - 双跑指标统计路径验证（total/error/p95/shadow）

### 集成测试

- 新增：`linux_deploy/smoke_dualrun_api.py`
  - 自动创建会话用户
  - 验证 dualrun_config GET/POST
  - 验证 dualrun_metrics 返回字段完整性

## 6. 变更清单

- `apps/crypto_screener/web/index.html`
- `apps/crypto_screener/web/style.css`
- `apps/crypto_screener/web/app.js`
- `apps/crypto_screener/app/web_server.py`
- `tests/test_dualrun_config.py`
- `linux_deploy/smoke_dualrun_api.py`

