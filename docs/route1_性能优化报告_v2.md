# 性能优化与架构调整方案 (Route 1 V2)

针对当前存在的4个核心问题，本文档提供完整的代码修改与架构调整方案。

## 1. 核心问题与解决方案

| 问题 | 原因分析 | 解决方案 |
| :--- | :--- | :--- |
| **(1) 主页等待 PKL** | 后端更新链路串行或阻塞；前端请求 `latest_enriched` 时若 PKL 未好，CSV 读取过慢导致超时/卡顿。 | **架构解耦**：`pipeline` 只负责 CSV+Snapshot (秒级)；Web 服务端并行触发 WeCom 推送 (CSV模式) 和 PKL 构建 (Parquet模式)。 |
| **(2) 企微发送无选中** | 推送线程启动时数据可能未就绪，或因 PKL 锁定导致读取失败；且筛选引擎缺少序列数据。 | **强制回退机制**：推送逻辑强制使用 `series_source` 的 CSV/Parquet 回退链路，不依赖 PKL，并增加重试与日志。 |
| **(3) PKL 生成慢 (8min)** | `factor_cache_update.py` 仍在使用 Pandas 读取大量 CSV 文本文件，IO 开销巨大。 | **Parquet 加速**：利用 `incremental_update.py` 产出的 Parquet (二进制/列式存储) 作为源数据，读取速度可提升 10-20 倍。 |
| **(4) 界面卡顿** | `app.js` 中 `computeBuiltins` 和 `applyAllFilters` 是同步计算，大量币种计算时阻塞 UI 主线程。 | **前端分片计算**：将计算逻辑改造为 `await` 异步分片执行，利用 `requestIdleCallback` 让出主线程。 |

---

## 2. 详细实施步骤

### 2.1 步骤一：优化更新流水线 (Pipeline Decoupling)

**目标**：确保 `run_once` 只做最快的事 (CSV抓取 + `latest.json` 生成)，其他全部异步。

**修改文件**: `apps/crypto_screener/app/pipeline.py`
*(保持现状即可，当前 `run_once` 已经只包含 fetch 和 snapshot。关键在于调用方)*

**修改文件**: `apps/crypto_screener/app/web_server.py`
*确保 `start_update` 里的逻辑是完全并行的。*

```python
# 修改 _run_update 函数 (约 L860 行)
def _run_update(fetch: bool) -> None:
    paths = default_paths()
    with run_lock:
        update_state["running"] = True
        update_state["last_started"] = datetime.now().isoformat(timespec="seconds")
        # ... (省略中间) ...
        try:
            # 1. 核心链路：抓取 + 快照 (耗时短，前端可见)
            run_once(paths, fetch=fetch)
            
            # 2. 立即启动并行任务
            # Task A: 企微推送 (使用刚生成的 CSV/Snapshot)
            threading.Thread(target=_send_wecom_for_all_enabled, daemon=True).start()
            
            # Task B: 增量预处理 (生成 Parquet，为 PKL 加速做准备)
            # 注意：PKL 构建依赖预处理的结果，所以这里是串行的：预处理 -> PKL
            try:
                from 数据获取.incremental_update import run_incremental_catchup
                cfg_path = ...
                # 运行预处理
                run_incremental_catchup(cfg_path, lag_hours=lag_h, max_hours=max_h)
                
                # Task C: PKL 缓存构建 (异步)
                if str(os.environ.get("QC_BUILD_PKL_CACHE") or "0").strip() != "0":
                    start_pkl_build_thread() # 封装启动逻辑
            except Exception as e:
                update_state["last_error_preprocess"] = str(e)
                
        except Exception as e:
            update_state["last_error"] = str(e)
        finally:
            update_state["running"] = False
            # ...
```

### 2.2 步骤二：修复企微推送 (Fix Notification)

**修改文件**: `apps/crypto_screener/app/web_server.py`

在 `_send_wecom_for_all_enabled` 中，确保能够读取到序列。问题通常出在 `_load_latest_rows` 读到了最新的 `latest.json`，但 `_attach_series_to_rows` 尝试读 PKL 失败（因为 PKL 正在构建中且被锁，或还未生成）。

**解决方案**：强制 `series_source` 在 PKL 不可用时回退到 CSV/Parquet。

```python
# 确保 series_source.py 中的 _load_series_from_pkl_cache 在 pkl_ready=False 时能正确返回 stale=True
# 当前代码逻辑已包含此回退，但在 web_server 中需要确保不被异常中断。

def _send_wecom_for_all_enabled() -> None:
    # 增加延时，确保 snapshot 文件写入落盘完全完成
    time.sleep(2.0) 
    try:
        # ... (现有逻辑)
        latest, all_rows = _load_latest_rows()
        if not all_rows:
            logger.error("[WeCom] No rows in latest.json")
            return
            
        # ... 遍历配置 ...
        # 关键修改：调用 _attach_series_to_rows 时，内部会自动处理回退。
        # 但我们需要确保 filter_engine 能处理可能存在的 None 值
        
        # 增加日志调试
        logger.info(f"[WeCom] Processing {len(all_rows)} rows for webhook...")
        
        rows0 = _attach_series_to_rows(rows=all_rows, config=config, tail=tail0)
        
        # 检查是否成功附加快照
        valid_series_count = sum(1 for r in rows0 if "series" in r and r["series"])
        logger.info(f"[WeCom] Rows with series attached: {valid_series_count}/{len(rows0)}")
        
        if valid_series_count == 0:
             logger.warning("[WeCom] No series data attached, skipping filtering to avoid empty result.")
             # 这里可以选择直接发送 Top N 的 Snapshot 数据，或者终止
             return

        r = apply_all_filters(rows0, config)
        # ...
```

### 2.3 步骤三：加速 PKL 构建 (Use Parquet)

**核心优化**：修改 `数据获取/factor_cache_update.py`，使其优先读取 `数据获取/data/preprocessed_hourly` 下的 Parquet 文件。

**修改文件**: `数据获取/factor_cache_update.py`

```python
# 新增函数：从 manifest 读取 Parquet 并合并
def _load_symbol_from_parquet(root: Path, market: str, symbol: str, tail: int) -> pd.DataFrame:
    # 使用 series_source 中已有的逻辑 (复用代码或重新实现简化版)
    # ... 读取 manifest.json 找到对应的 parquet 文件列表 ...
    # ... pd.read_pickle(parquet_path) ...
    # ... concat & tail ...
    pass

# 修改 _load_one_symbol 函数
def _load_one_symbol(..., preprocessed_root: Path | None = None, ...):
    # 1. 尝试从 Parquet 读取 (极速)
    if preprocessed_root:
        df = _load_symbol_from_parquet(preprocessed_root, market, symbol, tail + update_tail)
        if not df.empty:
             return ... # 处理并返回
             
    # 2. 回退到 CSV 读取 (现有逻辑)
    # ...
```

**配置变更**：
确保 `QC_USE_PREPROCESSED_SERIES=1` 且 `run_incremental_catchup` 在 PKL 构建前执行（目前流程已满足）。

### 2.4 步骤四：前端计算不卡顿 (Async UI)

**修改文件**: `apps/crypto_screener/web/app.js`

将同步循环改为异步分片。

```javascript
// 原代码 (伪代码)
// function applyAllFilters(rows, config) {
//    for (let row of rows) {
//       computeBuiltins(row); // 同步计算
//       checkFilters(row);
//    }
// }

// 新代码 (Async Chunking)
async function applyAllFiltersAsync(rows, config, onProgress) {
    const CHUNK_SIZE = 200; // 每批处理 200 个
    const results = [];
    
    for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
        const chunk = rows.slice(i, i + CHUNK_SIZE);
        
        // 处理当前分片
        for (let row of chunk) {
            // 如果缺少指标，在此处计算
            if (!row._builtins) {
                row._builtins = computeBuiltins(row); 
            }
            if (checkFilters(row, config)) {
                results.push(row);
            }
        }
        
        // 让出主线程，允许 UI 渲染
        if (i + CHUNK_SIZE < rows.length) {
            await new Promise(resolve => setTimeout(resolve, 0));
        }
        
        if (onProgress) onProgress(i + chunk.length, rows.length);
    }
    return results;
}

// 在点击“计算因子”按钮的事件处理中调用：
btn.onclick = async () => {
    showLoading();
    const result = await applyAllFiltersAsync(allRows, config, updateProgressBar);
    renderTable(result);
    hideLoading();
};
```

---

## 3. 执行脚本 (Deploy)

将上述修改落实到代码库后，使用以下命令重启服务验证：

```bash
# 1. 停止旧服务
pkill -f "web_server.py"

# 2. 启动服务 (使用新的 web_server.py)
nohup ./linux_deploy/run_web.sh > web.log 2>&1 &

# 3. 触发一次更新测试
curl -X POST http://127.0.0.1:8001/api/refresh?fetch=1 -H "Cookie: qc_sess=..."
```

## 4. 预期效果

1.  **主页秒开**：CSV 更新完后，主页立即显示最新价格（基于 `latest.json`），右上角显示“数据源: CSV (PKL 构建中)”。
2.  **推送正常**：企微在 CSV 更新后立即发送，包含正确选币。
3.  **构建飞快**：PKL 构建时间从 8 分钟缩短至 30-60 秒（依赖 Parquet 读取优化）。
4.  **操作丝滑**：点击计算因子时，界面不再假死，有进度条显示计算过程。
