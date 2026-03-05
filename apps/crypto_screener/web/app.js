const $ = (id) => document.getElementById(id);

const state = {
  meta: null,
  latest: null,
  customFactors: [],
  exprEditorOpen: false,
  exprDraftParams: [],
  lastAutoUpdateKey: null,
  followRefreshTimer: null,
  columnVisibility: {}, // { key: boolean }
  selectedKey: null, // `${market}|${symbol}`
  selectedRow: null,
  symbolQuery: "",
  klineSeriesCache: {},
  klineSeriesPending: {},
  baseConfig: null,
  strategyGroups: [],
  strategyDraft: null,
};

const storageKey = "crypto_screener_custom_factors_v1";
const columnVisibilityKey = "crypto_screener_column_visibility_v1";
const plotKeysStorageKey = "crypto_screener_plot_keys_v1";
const paramsStorageKey = "crypto_screener_params_v1";
const selectedKeyStorageKey = "crypto_screener_selected_key_v1";
const baseConfigStorageKey = "crypto_screener_base_config_v1";
const strategyGroupsStorageKey = "crypto_screener_strategy_groups_v1";
const baseCfgPaneStorageKey = "crypto_screener_basecfg_pane_v1";
const lastPicksStorageKey = "crypto_screener_last_picks_v1";

const defaultBlacklist = ["BKRW", "USDC", "USDP", "TUSD", "BUSD", "FDUSD", "DAI", "EUR", "GBP", "USBP", "SUSD", "PAXG", "AEUR", "EURI"];

function loadBaseConfig() {
  try {
    const raw = localStorage.getItem(baseConfigStorageKey);
    const j = raw ? JSON.parse(raw) : {};
    const wl = Array.isArray(j.whitelist) ? j.whitelist : [];
    const bl = Array.isArray(j.blacklist) ? j.blacklist : defaultBlacklist.slice();
    const market0 = String(j.market || "all");
    const market = market0 === "spot" || market0 === "swap" || market0 === "all" ? market0 : "all";
    return { whitelist: wl, blacklist: bl, market };
  } catch {
    return { whitelist: [], blacklist: defaultBlacklist.slice(), market: "all" };
  }
}

function saveBaseConfig(cfg) {
  try {
    const c = cfg && typeof cfg === "object" ? cfg : {};
    const wl = Array.isArray(c.whitelist) ? c.whitelist : [];
    const bl = Array.isArray(c.blacklist) ? c.blacklist : defaultBlacklist.slice();
    const market0 = String(c.market || "all");
    const market = market0 === "spot" || market0 === "swap" || market0 === "all" ? market0 : "all";
    localStorage.setItem(baseConfigStorageKey, JSON.stringify({ whitelist: wl, blacklist: bl, market }));
  } catch {}
}

function getSelectedMarket() {
  const cfg = state.baseConfig || loadBaseConfig();
  const market0 = String((cfg && cfg.market) || "all");
  if (market0 === "spot" || market0 === "swap" || market0 === "all") return market0;
  return "all";
}

function setSelectedMarket(market) {
  const v0 = String(market || "all");
  const v = v0 === "spot" || v0 === "swap" || v0 === "all" ? v0 : "all";
  const cfg0 = state.baseConfig || loadBaseConfig();
  const cfg = { ...(cfg0 || {}), market: v };
  state.baseConfig = cfg;
  saveBaseConfig(cfg);
  if ($("baseMarketSelect")) $("baseMarketSelect").value = v;
}

function loadBaseCfgPane() {
  try {
    const raw = localStorage.getItem(baseCfgPaneStorageKey);
    const v = String(raw || "");
    return v === "plans" || v === "market" || v === "strategy" || v === "push" ? v : "strategy";
  } catch {
    return "strategy";
  }
}

function saveBaseCfgPane(pane) {
  try {
    localStorage.setItem(baseCfgPaneStorageKey, String(pane || "strategy"));
  } catch {}
}

function setBaseCfgPane(pane) {
  const p0 = String(pane || "strategy");
  const p = p0 === "plans" || p0 === "market" || p0 === "strategy" || p0 === "push" ? p0 : "strategy";
  const tabs = [
    { id: "basecfgTabPlans", p: "plans" },
    { id: "basecfgTabMarket", p: "market" },
    { id: "basecfgTabStrategy", p: "strategy" },
    { id: "basecfgTabPush", p: "push" },
  ];
  for (const t of tabs) {
    const el = $(t.id);
    if (!el) continue;
    el.classList.toggle("active", t.p === p);
  }
  const panes = [
    { id: "basecfgPanePlans", p: "plans" },
    { id: "basecfgPaneMarket", p: "market" },
    { id: "basecfgPaneStrategy", p: "strategy" },
    { id: "basecfgPanePush", p: "push" },
  ];
  for (const it of panes) {
    const el = $(it.id);
    if (!el) continue;
    el.classList.toggle("hidden", it.p !== p);
  }
  saveBaseCfgPane(p);
}

function loadStrategyGroups() {
  try {
    const raw = localStorage.getItem(strategyGroupsStorageKey);
    const j = raw ? JSON.parse(raw) : [];
    return Array.isArray(j) ? j : [];
  } catch {
    return [];
  }
}

function saveStrategyGroups(gs) {
  try {
    localStorage.setItem(strategyGroupsStorageKey, JSON.stringify(Array.isArray(gs) ? gs : []));
  } catch {}
}

function saveLastPicks(rows, latestSummary) {
  try {
    const arr = Array.isArray(rows) ? rows : [];
    const out = {
      saved_at: new Date().toISOString(),
      summary: latestSummary && typeof latestSummary === "object" ? latestSummary : {},
      rows: arr.slice(0, 800).map((r) => ({
        rank: r && (r._rank ?? r.rank),
        symbol: r && r.symbol,
        market: r && r.market,
        close: r && r.close,
        pct_change: r && r.pct_change,
      })),
    };
    localStorage.setItem(lastPicksStorageKey, JSON.stringify(out));
  } catch {}
}

function deepCopy(x) {
  try {
    return JSON.parse(JSON.stringify(x === undefined ? null : x));
  } catch {
    return x;
  }
}

function normalizeStrategyConfig(cfg) {
  const c = cfg && typeof cfg === "object" ? cfg : {};
  const params = c.params && typeof c.params === "object" ? c.params : {};
  const toggles = c.toggles && typeof c.toggles === "object" ? c.toggles : {};
  const sort = c.sort && typeof c.sort === "object" ? c.sort : {};
  const lists0 = c.lists && typeof c.lists === "object" ? c.lists : {};
  const customFactors0 = Array.isArray(c.customFactors) ? c.customFactors : [];
  const cfMap = new Map(customFactors0.filter((x) => x && x.id).map((x) => [String(x.id), x]));
  const customFactors = (Array.isArray(state.customFactors) ? state.customFactors : []).map((f) => {
    const id = String(f && f.id ? f.id : "");
    const cur = id ? cfMap.get(id) : null;
    const params0 = Array.isArray(cur && cur.params) ? cur.params : Array.isArray(f && f.params) ? f.params : [];
    return {
      id,
      folder: cur && cur.folder ? cur.folder : f && f.folder ? f.folder : "默认",
      name: cur && cur.name ? cur.name : f && f.name ? f.name : id,
      template: String((cur && (cur.template || cur.expr)) || (f && (f.template || f.expr)) || ""),
      params: params0.map((x) => (Number.isFinite(Number(x)) ? Number(x) : 0)),
      enabled: cur && cur.enabled !== undefined ? !!cur.enabled : !!(f && f.enabled),
      thresholdEnabled: cur && cur.thresholdEnabled !== undefined ? !!cur.thresholdEnabled : !!(f && f.thresholdEnabled),
      cmp: String((cur && cur.cmp) || (f && f.cmp) || ">="),
      threshold: Number((cur && cur.threshold) !== undefined ? cur.threshold : (f && f.threshold) !== undefined ? f.threshold : 0),
      show: cur && cur.show !== undefined ? !!cur.show : !!(f && f.show !== false),
    };
  });
  return {
    params: { ...params },
    toggles: { ...toggles },
    sort: { key: String(sort.key || "pct_change"), order: String(sort.order || "desc") },
    customFactors,
    lists: {
      whitelist: Array.isArray(lists0.whitelist) ? lists0.whitelist : [],
      blacklist: Array.isArray(lists0.blacklist) ? lists0.blacklist : defaultBlacklist.slice(),
    },
  };
}

function strategyHasAnyCondition(cfg) {
  const c = normalizeStrategyConfig(cfg);
  const t = c.toggles || {};
  const anyBuiltIn = Object.values(t).some((x) => !!x);
  const anyCustom = Array.isArray(c.customFactors) && c.customFactors.some((f) => f && f.enabled);
  return anyBuiltIn || anyCustom;
}

function renderStrategyCondEditor() {
  const host = $("strategyCondEditor");
  if (!host) return;
  const cfg = normalizeStrategyConfig(state.strategyDraft || buildWecomConfigFromCurrent());
  state.strategyDraft = cfg;
  host.innerHTML = "";

  const makeLine = (labelText) => {
    const line = document.createElement("div");
    line.className = "help-line";
    line.textContent = labelText;
    return line;
  };

  const makeNumber = ({ value, min, max, step, onChange }) => {
    const inp = document.createElement("input");
    inp.className = "input";
    inp.type = "number";
    if (min !== undefined) inp.min = String(min);
    if (max !== undefined) inp.max = String(max);
    if (step !== undefined) inp.step = String(step);
    inp.value = String(Number.isFinite(Number(value)) ? Number(value) : "");
    inp.addEventListener("change", () => {
      const v = Number(inp.value);
      if (!Number.isFinite(v)) return;
      onChange(v);
      updateWecomSummary(state.strategyDraft);
    });
    return inp;
  };

  const makeSelect = ({ value, options, onChange }) => {
    const sel = document.createElement("select");
    sel.className = "select";
    for (const opt0 of options) {
      const opt = document.createElement("option");
      opt.value = opt0.value;
      opt.textContent = opt0.label;
      sel.appendChild(opt);
    }
    sel.value = String(value || "");
    sel.addEventListener("change", () => {
      onChange(sel.value);
      updateWecomSummary(state.strategyDraft);
    });
    return sel;
  };

  const builtins = [
    { key: "condCloseMa", title: "close > MA", params: [{ k: "maPeriodClose", name: "MA 周期", min: 2, step: 1 }] },
    { key: "condMa", title: "MA(快) > MA(慢)", params: [{ k: "maFast", name: "快", min: 2, step: 1 }, { k: "maSlow", name: "慢", min: 2, step: 1 }] },
    { key: "condEma", title: "close > EMA", params: [{ k: "emaPeriod", name: "EMA 周期", min: 2, step: 1 }] },
    { key: "condBollUp", title: "close > BOLLUP", params: [{ k: "bollPeriod", name: "周期", min: 2, step: 1 }, { k: "bollStd", name: "标准差", min: 0.1, step: 0.1 }] },
    { key: "condBollDown", title: "close < BOLLDOWN", params: [{ k: "bollDownPeriod", name: "周期", min: 2, step: 1 }, { k: "bollDownStd", name: "标准差", min: 0.1, step: 0.1 }] },
    { key: "condSuper", title: "close > SUPER", params: [{ k: "superAtrPeriod", name: "ATR 周期", min: 1, step: 1 }, { k: "superMult", name: "乘数", min: 0.1, step: 0.1 }] },
    { key: "condRsi", title: "RSI > 阈值", params: [{ k: "rsiPeriod", name: "RSI 周期", min: 2, step: 1 }, { k: "rsiThreshold", name: "阈值", min: 0, max: 100, step: 0.1 }] },
    { key: "condKdj", title: "KDJ(K > D)", params: [{ k: "kdjN", name: "N 周期", min: 1, step: 1 }, { k: "kdjM1", name: "M1 周期", min: 1, step: 1 }, { k: "kdjM2", name: "M2 周期", min: 1, step: 1 }] },
    { key: "condObv", title: "OBV > MA", params: [{ k: "obvMaPeriod", name: "MA 周期", min: 2, step: 1 }] },
    { key: "condStochRsi", title: "StochRSI(K > D)", params: [{ k: "stochRsiP", name: "RSI 周期", min: 2, step: 1 }, { k: "stochRsiK", name: "Stoch 周期", min: 2, step: 1 }, { k: "stochRsiSmK", name: "平滑 K", min: 1, step: 1 }, { k: "stochRsiSmD", name: "平滑 D", min: 1, step: 1 }] },
  ];

  for (const b of builtins) {
    const card = document.createElement("div");
    card.className = "cond";
    const top = document.createElement("div");
    top.className = "cond-top";
    const chk = document.createElement("label");
    chk.className = "check";
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = !!cfg.toggles[b.key];
    cb.addEventListener("change", () => {
      state.strategyDraft.toggles[b.key] = !!cb.checked;
      updateWecomSummary(state.strategyDraft);
    });
    chk.appendChild(cb);
    chk.appendChild(document.createTextNode(` ${b.title}`));
    top.appendChild(chk);

    const det = document.createElement("details");
    det.className = "details details-inline";
    const sum = document.createElement("summary");
    sum.textContent = "参数";
    det.appendChild(sum);
    const plist = document.createElement("div");
    plist.className = "param-list";
    for (const p of b.params) {
      const row = document.createElement("div");
      row.className = "param-item";
      const sp = document.createElement("span");
      sp.className = "label-inline";
      sp.textContent = p.name;
      row.appendChild(sp);
      row.appendChild(
        makeNumber({
          value: cfg.params[p.k],
          min: p.min,
          max: p.max,
          step: p.step,
          onChange: (v) => {
            state.strategyDraft.params[p.k] = v;
          },
        })
      );
      plist.appendChild(row);
    }
    det.appendChild(plist);
    top.appendChild(det);
    card.appendChild(top);
    host.appendChild(card);
  }

  host.appendChild(makeLine("自定义因子（从左侧已创建的因子中选择）"));
  if (!cfg.customFactors.length) {
    host.appendChild(makeLine("暂无自定义因子"));
    return;
  }

  for (const f of cfg.customFactors) {
    const card = document.createElement("div");
    card.className = "cond";
    const top = document.createElement("div");
    top.className = "cond-top";
    const chk = document.createElement("label");
    chk.className = "check";
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = !!f.enabled;
    cb.addEventListener("change", () => {
      f.enabled = !!cb.checked;
      updateWecomSummary(state.strategyDraft);
    });
    chk.appendChild(cb);
    chk.appendChild(document.createTextNode(` [${f.folder || "默认"}] ${f.name || f.id}`));
    top.appendChild(chk);

    const det = document.createElement("details");
    det.className = "details details-inline";
    const sum = document.createElement("summary");
    sum.textContent = "参数";
    det.appendChild(sum);
    const plist = document.createElement("div");
    plist.className = "param-list";

    if (Array.isArray(f.params) && f.params.length) {
      const row = document.createElement("div");
      row.className = "param-item";
      const sp = document.createElement("span");
      sp.className = "label-inline";
      sp.textContent = "n 参数";
      row.appendChild(sp);
      const wrap = document.createElement("div");
      wrap.style.display = "flex";
      wrap.style.gap = "8px";
      wrap.style.flexWrap = "wrap";
      for (let i = 0; i < f.params.length; i++) {
        const inp = makeNumber({
          value: f.params[i],
          step: 1,
          onChange: (v) => {
            f.params[i] = v;
          },
        });
        inp.style.width = "86px";
        inp.title = `n${i + 1}`;
        wrap.appendChild(inp);
      }
      row.appendChild(wrap);
      plist.appendChild(row);
    }

    const row2 = document.createElement("div");
    row2.className = "param-item";
    const sp2 = document.createElement("span");
    sp2.className = "label-inline";
    sp2.textContent = "阈值";
    row2.appendChild(sp2);
    const tWrap = document.createElement("div");
    tWrap.style.display = "flex";
    tWrap.style.gap = "8px";
    tWrap.style.flexWrap = "wrap";
    const thCbLabel = document.createElement("label");
    thCbLabel.className = "check";
    const thCb = document.createElement("input");
    thCb.type = "checkbox";
    thCb.checked = !!f.thresholdEnabled;
    thCb.addEventListener("change", () => {
      f.thresholdEnabled = !!thCb.checked;
      updateWecomSummary(state.strategyDraft);
    });
    thCbLabel.appendChild(thCb);
    thCbLabel.appendChild(document.createTextNode("启用"));
    tWrap.appendChild(thCbLabel);
    const cmpSel = makeSelect({
      value: f.cmp || ">=",
      options: [
        { value: ">=", label: ">=" },
        { value: ">", label: ">" },
        { value: "<=", label: "<=" },
        { value: "<", label: "<" },
      ],
      onChange: (v) => {
        f.cmp = v;
      },
    });
    cmpSel.style.width = "86px";
    tWrap.appendChild(cmpSel);
    const thrInp = makeNumber({
      value: f.threshold,
      step: 0.0001,
      onChange: (v) => {
        f.threshold = v;
      },
    });
    thrInp.style.width = "120px";
    tWrap.appendChild(thrInp);
    row2.appendChild(tWrap);
    plist.appendChild(row2);

    det.appendChild(plist);
    top.appendChild(det);
    card.appendChild(top);
    host.appendChild(card);
  }
}

function loadColumnVisibility() {
  try {
    const raw = localStorage.getItem(columnVisibilityKey);
    return raw ? JSON.parse(raw) : {};
  } catch {
    return {};
  }
}

function saveColumnVisibility() {
  localStorage.setItem(columnVisibilityKey, JSON.stringify(state.columnVisibility));
}

function savePlotKeys(keys) {
  try {
    localStorage.setItem(plotKeysStorageKey, JSON.stringify(keys));
  } catch {}
}

function saveParams(params) {
  try {
    localStorage.setItem(paramsStorageKey, JSON.stringify(params || {}));
  } catch {}
}

function showProgress(pct, text) {
  const overlay = $("progressOverlay");
  const fill = $("progressFill");
  const t = $("progressText");
  overlay.classList.remove("hidden");
  overlay.setAttribute("aria-hidden", "false");
  fill.style.width = `${Math.max(0, Math.min(100, pct))}%`;
  t.textContent = text || "处理中...";
}

function hideProgress() {
  const overlay = $("progressOverlay");
  overlay.classList.add("hidden");
  overlay.setAttribute("aria-hidden", "true");
}

function hideBootOverlay() {
  const el = $("bootOverlay");
  if (!el) return;
  el.classList.add("hidden");
  el.setAttribute("aria-hidden", "true");
}

function scheduleFollowRefresh(prevMetaUpdatedAt) {
  if (state.followRefreshTimer) return;
  const prev = prevMetaUpdatedAt ? String(prevMetaUpdatedAt) : "";
  const start = Date.now();
  const tick = async () => {
    try {
      const meta = await loadJson("./data/meta.json");
      const updatedAt = meta && meta.updated_at ? String(meta.updated_at) : "";
      if (updatedAt && updatedAt !== prev) {
        state.followRefreshTimer = null;
        state.meta = meta;
        refresh({ auto: true, skipBackend: true });
        return;
      }
    } catch {}
    if (Date.now() - start >= 30000) {
      state.followRefreshTimer = null;
      return;
    }
    state.followRefreshTimer = setTimeout(tick, 900);
  };
  state.followRefreshTimer = setTimeout(tick, 900);
}

function setStatus(text) {
  const el = $("status");
  if (!el) return;
  el.textContent = text || "";
}

function refreshFactors() {
  return refresh({ skipBackend: true, fetchMode: false, factorCompute: true, forceEnriched: true });
}

function fmtNum(v) {
  if (v === null || v === undefined || Number.isNaN(v)) return "";
  const n = Number(v);
  if (!Number.isFinite(n)) return "";
  if (Math.abs(n) >= 1000) return n.toFixed(2);
  if (Math.abs(n) >= 1) return n.toFixed(4);
  return n.toFixed(6);
}

function fmtDt(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  return `${y}-${m}-${dd} ${hh}:${mm}`;
}

function rowKey(r) {
  const m = String(r && r.market ? r.market : "");
  const s = String(r && r.symbol ? r.symbol : "");
  return `${m}|${s}`;
}

function syncSelectedRowFrom(rows) {
  if (!Array.isArray(rows) || !rows.length) {
    state.selectedRow = null;
    state.selectedKey = null;
    renderKline(null);
    return;
  }
  const curKey = state.selectedKey;
  const found = curKey ? rows.find((r) => rowKey(r) === curKey) : null;
  const pick = found || rows[0];
  state.selectedKey = rowKey(pick);
  state.selectedRow = pick;
  renderKline(pick);
  ensureKlineSeriesForRow(pick);
  applySelectedToTable();
}

async function loadKlineFromApi(market, symbol, tail) {
  const u = `./api/kline?market=${encodeURIComponent(String(market || ""))}&symbol=${encodeURIComponent(String(symbol || ""))}&tail=${encodeURIComponent(String(tail || 360))}`;
  const res = await fetch(u, { cache: "no-store" });
  if (res.status === 401) {
    const next = encodeURIComponent(location.pathname + location.search);
    location.href = `./login.html?next=${next}`;
    throw new Error("未登录");
  }
  if (!res.ok) throw new Error(`kline api: ${res.status}`);
  const json = await res.json();
  if (!json || !json.ok) throw new Error(`kline api: ${String((json || {}).error || "bad_response")}`);
  return json;
}

function ensureKlineSeriesForRow(row) {
  try {
    if (!row) return;
    const key = rowKey(row);
    if (!key) return;
    if (row.series && row.series.close && Array.isArray(row.series.close) && row.series.close.length) return;
    const cached = state.klineSeriesCache && state.klineSeriesCache[key] ? state.klineSeriesCache[key] : null;
    if (cached && cached.series) {
      row.series = cached.series;
      if (state.selectedKey === key) renderKline(row);
      return;
    }
    if (state.klineSeriesPending && state.klineSeriesPending[key]) return;
    let tail = 720;
    try {
      const t0 = Number((state.latest && state.latest.config && state.latest.config.tail_len) || 720);
      if (Number.isFinite(t0) && t0 > 0) tail = t0;
    } catch {}
    const p = (async () => {
      try {
        const api = await loadKlineFromApi(String(row.market), String(row.symbol), Math.min(3650, Math.max(60, Math.trunc(tail))));
        const dt = Array.isArray(api.dt) ? api.dt : [];
        const series = api && api.series && typeof api.series === "object" ? api.series : {};
        if (!state.klineSeriesCache) state.klineSeriesCache = {};
        state.klineSeriesCache[key] = { dt, series };
        row.series = series;
        if (state.selectedKey === key) renderKline(row);
      } catch {}
      try {
        if (state.klineSeriesPending) delete state.klineSeriesPending[key];
      } catch {}
    })();
    if (!state.klineSeriesPending) state.klineSeriesPending = {};
    state.klineSeriesPending[key] = p;
  } catch {}
}

function applySelectedToTable() {
  const tbody = $("tbody");
  if (!tbody) return;
  const key = state.selectedKey;
  for (const tr of tbody.querySelectorAll("tr")) {
    const rk = tr.getAttribute("data-rk");
    tr.classList.toggle("selected", !!key && rk === key);
  }
}

function ensureCanvasSize(canvas) {
  if (!canvas) return;
  const rect = canvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  const w = Math.max(10, Math.round(rect.width * dpr));
  const h = Math.max(10, Math.round(rect.height * dpr));
  if (canvas.width !== w) canvas.width = w;
  if (canvas.height !== h) canvas.height = h;
}

function renderKline(row) {
  const canvas = $("klineCanvas");
  const meta = $("klineMeta");
  if (!canvas || !meta) return;
  ensureCanvasSize(canvas);
  const ctx = canvas.getContext("2d");
  const w = canvas.width;
  const h = canvas.height;
  ctx.clearRect(0, 0, w, h);

  if (!row || !row.series) {
    meta.textContent = "点击右侧表格中的币种查看K线";
    ctx.fillStyle = getComputedStyle(document.body).getPropertyValue("--text-dim").trim() || "#94a3b8";
    ctx.font = `${Math.max(12, Math.round(12 * (window.devicePixelRatio || 1)))}px sans-serif`;
    ctx.fillText("暂无数据", 12, 24);
    return;
  }

  const series = row.series || {};
  const opens = Array.isArray(series.open) ? series.open : [];
  const highs = Array.isArray(series.high) ? series.high : [];
  const lows = Array.isArray(series.low) ? series.low : [];
  const closes = Array.isArray(series.close) ? series.close : [];
  const n0 = Math.min(opens.length, highs.length, lows.length, closes.length);
  if (!n0) {
    meta.textContent = `${stripQuote(row.symbol)}（${marketLabel(row.market)}）暂无K线序列`;
    return;
  }

  const viewN = Math.min(360, n0);
  const start = n0 - viewN;

  let minP = Infinity;
  let maxP = -Infinity;
  for (let i = start; i < n0; i++) {
    const hi = Number(highs[i]);
    const lo = Number(lows[i]);
    if (Number.isFinite(hi)) maxP = Math.max(maxP, hi);
    if (Number.isFinite(lo)) minP = Math.min(minP, lo);
  }
  if (!Number.isFinite(minP) || !Number.isFinite(maxP) || minP === maxP) {
    meta.textContent = `${stripQuote(row.symbol)}（${marketLabel(row.market)}）K线数据异常`;
    return;
  }

  const pad = (maxP - minP) * 0.06;
  const yMax = maxP + pad;
  const yMin = minP - pad;
  const plotL = 10;
  const plotR = 8;
  const plotT = 8;
  const plotB = 18;
  const pw = w - plotL - plotR;
  const ph = h - plotT - plotB;
  const stepX = pw / viewN;
  const bodyW = Math.max(1, Math.min(12, Math.floor(stepX * 0.65)));

  const cs = getComputedStyle(document.body);
  const upColor = (cs.getPropertyValue("--success").trim() || "#10b981");
  const dnColor = (cs.getPropertyValue("--danger").trim() || "#ef4444");
  const gridColor = (cs.getPropertyValue("--border").trim() || "rgba(255,255,255,0.1)");
  const textColor = (cs.getPropertyValue("--text-dim").trim() || "#94a3b8");

  const yOf = (p) => plotT + ((yMax - p) / (yMax - yMin)) * ph;

  ctx.strokeStyle = gridColor;
  ctx.lineWidth = 1;
  ctx.beginPath();
  for (let k = 1; k <= 4; k++) {
    const yy = plotT + (ph * k) / 5;
    ctx.moveTo(plotL, yy);
    ctx.lineTo(plotL + pw, yy);
  }
  ctx.stroke();

  for (let i = 0; i < viewN; i++) {
    const idx = start + i;
    const o = Number(opens[idx]);
    const hi = Number(highs[idx]);
    const lo = Number(lows[idx]);
    const c = Number(closes[idx]);
    if (!Number.isFinite(o) || !Number.isFinite(hi) || !Number.isFinite(lo) || !Number.isFinite(c)) continue;
    const up = c >= o;
    const xMid = plotL + i * stepX + stepX / 2;
    const yO = yOf(o);
    const yC = yOf(c);
    const yH = yOf(hi);
    const yL = yOf(lo);
    const color = up ? upColor : dnColor;

    ctx.strokeStyle = color;
    ctx.beginPath();
    ctx.moveTo(xMid, yH);
    ctx.lineTo(xMid, yL);
    ctx.stroke();

    ctx.fillStyle = color;
    const top = Math.min(yO, yC);
    const bot = Math.max(yO, yC);
    const bh = Math.max(1, bot - top);
    ctx.fillRect(Math.round(xMid - bodyW / 2), Math.round(top), bodyW, Math.round(bh));
  }

  const lastIdx = n0 - 1;
  const lastClose = Number(closes[lastIdx]);
  if (Number.isFinite(lastClose)) {
    ctx.strokeStyle = (cs.getPropertyValue("--accent").trim() || "#3b82f6");
    ctx.setLineDash([4, 4]);
    ctx.beginPath();
    ctx.moveTo(plotL, yOf(lastClose));
    ctx.lineTo(plotL + pw, yOf(lastClose));
    ctx.stroke();
    ctx.setLineDash([]);
  }

  ctx.fillStyle = textColor;
  ctx.font = `${Math.max(12, Math.round(12 * (window.devicePixelRatio || 1)))}px sans-serif`;
  ctx.fillText(fmtNum(yMax), plotL, plotT + 12);
  ctx.fillText(fmtNum(yMin), plotL, plotT + ph + 12);

  meta.textContent = `${stripQuote(row.symbol)}（${marketLabel(row.market)}）｜ K线：${viewN}/${n0} ｜ 最新：${fmtDt(row.dt_display)} ｜ 收盘：${fmtNum(row.close)}`;
}

async function loadJson(url) {
  const u = `${url}?t=${Date.now()}`;
  const res = await fetch(u, { cache: "no-store" });
  if (res.status === 401) {
    const next = encodeURIComponent(location.pathname + location.search);
    location.href = `./login.html?next=${next}`;
    throw new Error("未登录");
  }
  if (!res.ok) throw new Error(`请求失败：${res.status}`);
  return await res.json();
}

async function loadStatus() {
  try {
    return await loadJson("./api/status");
  } catch {
    return null;
  }
}

async function waitBackendDone({ timeoutMs, pollMs } = {}) {
  const t = Math.max(0, Number(timeoutMs || 0));
  const step = Math.max(300, Number(pollMs || 0) || 1000);
  const start = Date.now();
  while (true) {
    const st = await loadStatus();
    const running = !!(st && st.update && st.update.running);
    if (!running) return st;
    if (t && Date.now() - start >= t) return st;
    await new Promise((r) => setTimeout(r, step));
  }
}

async function triggerBackendRefresh({ timeoutMs, fetchMode } = {}) {
  const t = Math.max(0, Number(timeoutMs || 0));
  const ctrl = t ? new AbortController() : null;
  let timer = null;
  if (ctrl) timer = setTimeout(() => ctrl.abort(), t);
  try {
    const fetchArg = fetchMode === false ? "0" : "1";
    const r = await fetch(`./api/refresh?fetch=${fetchArg}`, { method: "POST", signal: ctrl ? ctrl.signal : undefined });
    return r;
  } catch (e) {
    return null;
  } finally {
    if (timer) clearTimeout(timer);
  }
}

function loadCustomFactors() {
  try {
    const raw = localStorage.getItem(storageKey);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.map((x) => {
      const out = { ...x };
      if (out.enabled === undefined) out.enabled = true;
      if (out.thresholdEnabled === undefined && out.enabled !== undefined && x.thresholdEnabled === undefined) {
        out.thresholdEnabled = !!x.enabled;
      }
      if (out.thresholdEnabled === undefined) out.thresholdEnabled = false;
      if (out.template === undefined && out.expr !== undefined) out.template = String(out.expr);
      if (!Array.isArray(out.params)) out.params = [];
      if (out.cmp === undefined) out.cmp = ">=";
      if (out.threshold === undefined) out.threshold = 0;
      if (out.folder === undefined) out.folder = "默认";
      if (out.name === undefined) out.name = "expr";
      if (out.show === undefined) out.show = true;
      return out;
    });
  } catch {
    return [];
  }
}

function saveCustomFactors(factors) {
  localStorage.setItem(storageKey, JSON.stringify(factors));
}

async function syncCustomFactorsToServer(factors) {
  try {
    await postJson("./api/custom_factors/replace", { factors: Array.isArray(factors) ? factors : [] });
  } catch {}
}

function newId() {
  return `${Date.now()}_${Math.random().toString(16).slice(2)}`;
}

function parseIntInput(el) {
  const v = Number(el.value);
  return Number.isFinite(v) ? Math.trunc(v) : NaN;
}

function parseFloatInput(el) {
  const v = Number(el.value);
  return Number.isFinite(v) ? v : NaN;
}

function setInvalid(el, invalid) {
  if (!el) return;
  if (invalid) el.classList.add("invalid");
  else el.classList.remove("invalid");
}

function getSeries(row, name) {
  const s = row && row.series ? row.series[name] : null;
  return Array.isArray(s) ? s : [];
}

function lastNonNull(arr) {
  for (let i = arr.length - 1; i >= 0; i--) {
    const v = arr[i];
    if (v !== null && v !== undefined && Number.isFinite(Number(v))) return Number(v);
  }
  return null;
}

function sma(arr, window) {
  const w = Math.trunc(Number(window));
  if (!Number.isFinite(w) || w <= 0) return null;
  if (!Array.isArray(arr) || arr.length < w) return null;
  let sum = 0;
  for (let i = arr.length - w; i < arr.length; i++) {
    const v = arr[i];
    if (v === null || v === undefined || !Number.isFinite(Number(v))) return null;
    sum += Number(v);
  }
  return sum / w;
}

function ema(arr, window) {
  const w = Math.trunc(Number(window));
  if (!Number.isFinite(w) || w <= 0) return null;
  const xs = Array.isArray(arr) ? arr.filter((v) => v !== null && v !== undefined && Number.isFinite(Number(v))).map(Number) : [];
  if (xs.length < w) return null;
  const alpha = 2 / (w + 1);
  let e = xs[0];
  for (let i = 1; i < xs.length; i++) e = alpha * xs[i] + (1 - alpha) * e;
  return e;
}

function emaSeries(arr, window) {
  const w = Math.trunc(Number(window));
  if (!Number.isFinite(w) || w <= 0) return [];
  const out = new Array(arr.length).fill(null);
  const alpha = 2 / (w + 1);
  let firstIdx = -1;
  for (let i = 0; i < arr.length; i++) {
    if (arr[i] !== null && arr[i] !== undefined && Number.isFinite(Number(arr[i]))) {
      firstIdx = i;
      break;
    }
  }
  if (firstIdx === -1) return out;
  let lastEma = Number(arr[firstIdx]);
  out[firstIdx] = lastEma;
  for (let i = firstIdx + 1; i < arr.length; i++) {
    const v = Number(arr[i]);
    if (arr[i] !== null && arr[i] !== undefined && Number.isFinite(v)) {
      lastEma = alpha * v + (1 - alpha) * lastEma;
      out[i] = lastEma;
    } else {
      out[i] = lastEma;
    }
  }
  return out;
}

function rollingMax(arr, window) {
  const w = Math.trunc(Number(window));
  if (!Number.isFinite(w) || w <= 0 || arr.length < w) return null;
  let max = -Infinity;
  for (let i = arr.length - w; i < arr.length; i++) {
    const v = Number(arr[i]);
    if (v > max) max = v;
  }
  return max;
}

function rollingMin(arr, window) {
  const w = Math.trunc(Number(window));
  if (!Number.isFinite(w) || w <= 0 || arr.length < w) return null;
  let min = Infinity;
  for (let i = arr.length - w; i < arr.length; i++) {
    const v = Number(arr[i]);
    if (v < min) min = v;
  }
  return min;
}

function atr(highs, lows, closes, window) {
  const w = Math.trunc(Number(window));
  if (closes.length < w + 1) return null;
  const trs = [];
  for (let i = 1; i < closes.length; i++) {
    const h = Number(highs[i]);
    const l = Number(lows[i]);
    const pc = Number(closes[i - 1]);
    trs.push(Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc)));
  }
  // RMA (Exponential Moving Average with alpha = 1/w)
  const alpha = 1 / w;
  let val = trs[0];
  for (let i = 1; i < trs.length; i++) {
    val = alpha * trs[i] + (1 - alpha) * val;
  }
  return val;
}

function supertrend(highs, lows, closes, period, mult) {
  const p = Math.trunc(Number(period));
  const m = Number(mult);
  if (closes.length < p + 1) return null;

  const trs = [];
  for (let i = 1; i < closes.length; i++) {
    const h = Number(highs[i]);
    const l = Number(lows[i]);
    const pc = Number(closes[i - 1]);
    trs.push(Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc)));
  }

  const alpha = 1 / p;
  const atrs = new Array(closes.length).fill(null);
  let currentAtr = trs[0];
  for (let i = 1; i < trs.length; i++) {
    currentAtr = alpha * trs[i] + (1 - alpha) * currentAtr;
    atrs[i + 1] = currentAtr;
  }

  let trend = 1;
  let upperBand = 0;
  let lowerBand = 0;
  let superTrend = 0;

  for (let i = p; i < closes.length; i++) {
    const mid = (Number(highs[i]) + Number(lows[i])) / 2;
    const a = atrs[i];
    if (a === null) continue;

    let basicUpper = mid + m * a;
    let basicLower = mid - m * a;

    const prevClose = Number(closes[i - 1]);
    upperBand = (basicUpper < upperBand || prevClose > upperBand) ? basicUpper : upperBand;
    lowerBand = (basicLower > lowerBand || prevClose < lowerBand) ? basicLower : lowerBand;

    if (trend === 1) {
      if (Number(closes[i]) < lowerBand) {
        trend = -1;
        superTrend = upperBand;
      } else {
        superTrend = lowerBand;
      }
    } else {
      if (Number(closes[i]) > upperBand) {
        trend = 1;
        superTrend = lowerBand;
      } else {
        superTrend = upperBand;
      }
    }
  }
  return superTrend;
}

function kdj(highs, lows, closes, n, m1, m2) {
  const period = Math.trunc(n);
  if (closes.length < period) return { k: null, d: null, j: null };
  
  const rsvs = [];
  for (let i = period - 1; i < closes.length; i++) {
    const c = Number(closes[i]);
    const h = Math.max(...highs.slice(i - period + 1, i + 1).map(Number));
    const l = Math.min(...lows.slice(i - period + 1, i + 1).map(Number));
    rsvs.push(h === l ? 50 : ((c - l) / (h - l)) * 100);
  }

  const emaK = (data, m) => {
    const alpha = 1 / m;
    let val = 50;
    for (const v of data) val = alpha * v + (1 - alpha) * val;
    return val;
  };

  const kSeries = [];
  let lastK = 50;
  const alpha1 = 1 / m1;
  for (const r of rsvs) {
    lastK = alpha1 * r + (1 - alpha1) * lastK;
    kSeries.push(lastK);
  }

  const alpha2 = 1 / m2;
  let lastD = 50;
  for (const k of kSeries) lastD = alpha2 * k + (1 - alpha2) * lastD;

  const currentK = kSeries[kSeries.length - 1];
  const currentD = lastD;
  const currentJ = 3 * currentK - 2 * currentD;

  return { k: currentK, d: currentD, j: currentJ };
}

function obv(closes, volumes) {
  if (closes.length < 2) return 0;
  let obvVal = 0;
  for (let i = 1; i < closes.length; i++) {
    const c = Number(closes[i]);
    const pc = Number(closes[i - 1]);
    const v = Number(volumes[i]);
    if (c > pc) obvVal += v;
    else if (c < pc) obvVal -= v;
  }
  return obvVal;
}

function obvWithMa(closes, volumes, maPeriod) {
  const p = Math.trunc(maPeriod);
  if (closes.length < p + 1) return { obv: null, ma: null };
  const obvSeries = [0];
  let currentObv = 0;
  for (let i = 1; i < closes.length; i++) {
    const c = Number(closes[i]);
    const pc = Number(closes[i - 1]);
    const v = Number(volumes[i]);
    if (c > pc) currentObv += v;
    else if (c < pc) currentObv -= v;
    obvSeries.push(currentObv);
  }
  const currentObvVal = obvSeries[obvSeries.length - 1];
  let sum = 0;
  for (let i = obvSeries.length - p; i < obvSeries.length; i++) sum += obvSeries[i];
  return { obv: currentObvVal, ma: sum / p };
}

function stochRsi(closes, rsiP, stochP, smoothK, smoothD) {
  const p = Math.trunc(rsiP);
  if (closes.length < p + 1) return { k: null, d: null };

  const rsiSeries = [];
  for (let i = p; i <= closes.length; i++) {
    rsiSeries.push(rsi(closes.slice(0, i), p));
  }

  const sP = Math.trunc(stochP);
  if (rsiSeries.length < sP) return { k: null, d: null };

  const stochSeries = [];
  for (let i = sP - 1; i < rsiSeries.length; i++) {
    const sub = rsiSeries.slice(i - sP + 1, i + 1);
    const low = Math.min(...sub);
    const high = Math.max(...sub);
    stochSeries.push(high === low ? 0 : ((rsiSeries[i] - low) / (high - low)) * 100);
  }

  const kSeries = emaSeries(stochSeries, smoothK);
  const dSeries = emaSeries(kSeries, smoothD);

  return {
    k: kSeries[kSeries.length - 1],
    d: dSeries[dSeries.length - 1]
  };
}

function rsi(arr, period) {
  const p = Math.trunc(Number(period));
  if (!Number.isFinite(p) || p <= 0) return null;
  const xs = Array.isArray(arr) ? arr.filter((v) => v !== null && v !== undefined && Number.isFinite(Number(v))).map(Number) : [];
  if (xs.length < p + 1) return null;
  const deltas = [];
  for (let i = 1; i < xs.length; i++) deltas.push(xs[i] - xs[i - 1]);
  if (deltas.length < p) return null;
  let avgGain = 0;
  let avgLoss = 0;
  for (let i = 0; i < p; i++) {
    const d = deltas[i];
    avgGain += d > 0 ? d : 0;
    avgLoss += d < 0 ? -d : 0;
  }
  avgGain /= p;
  avgLoss /= p;
  for (let i = p; i < deltas.length; i++) {
    const d = deltas[i];
    const g = d > 0 ? d : 0;
    const l = d < 0 ? -d : 0;
    avgGain = (avgGain * (p - 1) + g) / p;
    avgLoss = (avgLoss * (p - 1) + l) / p;
  }
  if (avgLoss === 0 && avgGain === 0) return 0;
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - 100 / (1 + rs);
}

function tokenizeExpr(src) {
  const s = String(src || "");
  const out = [];
  let i = 0;
  const isDigit = (c) => c >= "0" && c <= "9";
  const isAlpha = (c) => (c >= "a" && c <= "z") || (c >= "A" && c <= "Z") || c === "_";
  while (i < s.length) {
    const c = s[i];
    if (c === " " || c === "\t" || c === "\n" || c === "\r") {
      i++;
      continue;
    }
    if (c === ">" || c === "<" || c === "=" || c === "!") {
      const n = s[i + 1] || "";
      if (n === "=") {
        out.push({ t: `${c}=`, v: `${c}=` });
        i += 2;
        continue;
      }
      if (c === "=") throw new Error("不支持单独的 =");
      out.push({ t: c, v: c });
      i++;
      continue;
    }
    if (isDigit(c) || (c === "." && isDigit(s[i + 1] || ""))) {
      let j = i + 1;
      while (j < s.length && (isDigit(s[j]) || s[j] === ".")) j++;
      out.push({ t: "num", v: Number(s.slice(i, j)) });
      i = j;
      continue;
    }
    if (isAlpha(c)) {
      let j = i + 1;
      while (j < s.length && (isAlpha(s[j]) || isDigit(s[j]))) j++;
      out.push({ t: "id", v: s.slice(i, j) });
      i = j;
      continue;
    }
    if ("+-*/(),.".includes(c)) {
      out.push({ t: c, v: c });
      i++;
      continue;
    }
    throw new Error(`不支持字符：${c}`);
  }
  return out;
}

function parseExprTokens(tokens) {
  let pos = 0;
  const peek = () => tokens[pos];
  const take = (t) => {
    const cur = tokens[pos];
    if (!cur || (t && cur.t !== t)) throw new Error("表达式解析失败");
    pos++;
    return cur;
  };

  const parseAtom = () => {
    const cur = peek();
    if (!cur) throw new Error("表达式为空");
    if (cur.t === "num") {
      take("num");
      return { k: "num", v: cur.v };
    }
    if (cur.t === "id") {
      take("id");
      const id = cur.v;
      if (peek() && peek().t === "(") {
        take("(");
        const args = [];
        if (peek() && peek().t !== ")") {
          args.push(parseAddSub());
          while (peek() && peek().t === ",") {
            take(",");
            args.push(parseAddSub());
          }
        }
        take(")");
        return { k: "call", name: id, args };
      }
      return { k: "id", name: id };
    }
    if (cur.t === "(") {
      take("(");
      const e = parseAddSub();
      take(")");
      return e;
    }
    throw new Error("表达式解析失败");
  };

  const parsePostfix = () => {
    let node = parseAtom();
    while (peek() && peek().t === ".") {
      take(".");
      const m = take("id").v;
      if (peek() && peek().t === "(") {
        take("(");
        const args = [node];
        if (peek() && peek().t !== ")") {
          args.push(parseAddSub());
          while (peek() && peek().t === ",") {
            take(",");
            args.push(parseAddSub());
          }
        }
        take(")");
        node = { k: "call", name: m, args };
      } else {
        node = { k: "member", obj: node, prop: m };
      }
    }
    return node;
  };

  const parseUnary = () => {
    const cur = peek();
    if (cur && (cur.t === "+" || cur.t === "-")) {
      take(cur.t);
      return { k: "unary", op: cur.t, x: parseUnary() };
    }
    return parsePostfix();
  };

  const parseMulDiv = () => {
    let node = parseUnary();
    while (peek() && (peek().t === "*" || peek().t === "/")) {
      const op = take().t;
      node = { k: "bin", op, a: node, b: parseUnary() };
    }
    return node;
  };

  const parseAddSub = () => {
    let node = parseMulDiv();
    while (peek() && (peek().t === "+" || peek().t === "-")) {
      const op = take().t;
      node = { k: "bin", op, a: node, b: parseMulDiv() };
    }
    return node;
  };

  const parseCmp = () => {
    let node = parseAddSub();
    while (peek() && (peek().t === ">" || peek().t === "<" || peek().t === ">=" || peek().t === "<=" || peek().t === "==" || peek().t === "!=")) {
      const op = take().t;
      node = { k: "cmp", op, a: node, b: parseAddSub() };
    }
    return node;
  };

  const ast = parseCmp();
  if (pos !== tokens.length) throw new Error("表达式解析失败");
  return ast;
}

function vScalar(x) {
  const n = Number(x);
  return { t: "scalar", v: Number.isFinite(n) ? n : null };
}

function vSeries(arr) {
  return { t: "series", v: Array.isArray(arr) ? arr : [] };
}

function isSeriesVal(v) {
  return v && v.t === "series";
}

function scalarFrom(v) {
  if (!v) return null;
  if (v.t === "scalar") return v.v;
  if (v.t === "series") return lastNonNull(v.v);
  return null;
}

function seriesFrom(v, lenHint) {
  if (!v) return [];
  if (v.t === "series") return v.v;
  const s = v.t === "scalar" ? v.v : null;
  const n = Number.isFinite(Number(lenHint)) ? lenHint : 0;
  if (!n) return [];
  const out = new Array(n);
  for (let i = 0; i < n; i++) out[i] = s;
  return out;
}

function rollingStd(series, window) {
  const w = Math.trunc(Number(window));
  if (!Number.isFinite(w) || w <= 0) return null;
  if (!Array.isArray(series) || series.length < w) return null;
  const tail = series.slice(series.length - w);
  if (tail.some((x) => x === null || x === undefined || !Number.isFinite(Number(x)))) return null;
  const xs = tail.map(Number);
  const mean = xs.reduce((a, b) => a + b, 0) / w;
  const var0 = xs.reduce((a, b) => a + (b - mean) * (b - mean), 0) / w;
  return Math.sqrt(var0);
}

function rollingCorr(x, y, window) {
  const w = Math.trunc(Number(window));
  if (!Number.isFinite(w) || w <= 1) return null;
  if (!Array.isArray(x) || !Array.isArray(y)) return null;
  const n = Math.min(x.length, y.length);
  if (n < w) return null;
  const xs = x.slice(n - w);
  const ys = y.slice(n - w);
  for (let i = 0; i < w; i++) {
    if (!Number.isFinite(Number(xs[i])) || !Number.isFinite(Number(ys[i]))) return null;
  }
  const ax = xs.map(Number);
  const ay = ys.map(Number);
  const mx = ax.reduce((a, b) => a + b, 0) / w;
  const my = ay.reduce((a, b) => a + b, 0) / w;
  let cov = 0;
  let vx = 0;
  let vy = 0;
  for (let i = 0; i < w; i++) {
    const dx = ax[i] - mx;
    const dy = ay[i] - my;
    cov += dx * dy;
    vx += dx * dx;
    vy += dy * dy;
  }
  if (vx === 0 || vy === 0) return null;
  return cov / Math.sqrt(vx * vy);
}

function evalAst(node, ctx) {
  if (node.k === "num") return vScalar(node.v);
  if (node.k === "id") {
    const name = String(node.name || "").toLowerCase();
    if (["open", "high", "low", "close", "volume", "quote_volume"].includes(name)) return vSeries(ctx.series[name] || []);
    if (name === "quotevolume") return vSeries(ctx.series.quote_volume || []);
    if (name === "eps") return vScalar(1e-12);
    return vScalar(null);
  }
  if (node.k === "member") return vScalar(null);
  if (node.k === "unary") {
    const x = evalAst(node.x, ctx);
    if (isSeriesVal(x)) {
      const out = x.v.map((v) => (v === null || v === undefined || !Number.isFinite(Number(v)) ? null : (node.op === "-" ? -Number(v) : Number(v))));
      return vSeries(out);
    }
    const v = scalarFrom(x);
    if (v === null) return vScalar(null);
    return vScalar(node.op === "-" ? -v : v);
  }
  if (node.k === "bin" || node.k === "cmp") {
    const a = evalAst(node.a, ctx);
    const b = evalAst(node.b, ctx);
    const aIsS = isSeriesVal(a);
    const bIsS = isSeriesVal(b);
    const len = aIsS ? a.v.length : bIsS ? b.v.length : 0;
    const as = aIsS ? a.v : seriesFrom(a, len);
    const bs = bIsS ? b.v : seriesFrom(b, len);

    if (!len) {
      const av = scalarFrom(a);
      const bv = scalarFrom(b);
      if (av === null || bv === null) return vScalar(null);
      if (node.k === "bin") {
        if (node.op === "+") return vScalar(av + bv);
        if (node.op === "-") return vScalar(av - bv);
        if (node.op === "*") return vScalar(av * bv);
        if (node.op === "/") return vScalar(bv === 0 ? null : av / bv);
      } else {
        if (node.op === ">") return vScalar(av > bv ? 1 : 0);
        if (node.op === ">=") return vScalar(av >= bv ? 1 : 0);
        if (node.op === "<") return vScalar(av < bv ? 1 : 0);
        if (node.op === "<=") return vScalar(av <= bv ? 1 : 0);
        if (node.op === "==") return vScalar(av === bv ? 1 : 0);
        if (node.op === "!=") return vScalar(av !== bv ? 1 : 0);
      }
      return vScalar(null);
    }

    const out = new Array(len);
    for (let i = 0; i < len; i++) {
      const av = as[i];
      const bv = bs[i];
      if (!Number.isFinite(Number(av)) || !Number.isFinite(Number(bv))) {
        out[i] = null;
        continue;
      }
      const x = Number(av);
      const y = Number(bv);
      if (node.k === "bin") {
        if (node.op === "+") out[i] = x + y;
        else if (node.op === "-") out[i] = x - y;
        else if (node.op === "*") out[i] = x * y;
        else if (node.op === "/") out[i] = y === 0 ? null : x / y;
        else out[i] = null;
      } else {
        if (node.op === ">") out[i] = x > y ? 1 : 0;
        else if (node.op === ">=") out[i] = x >= y ? 1 : 0;
        else if (node.op === "<") out[i] = x < y ? 1 : 0;
        else if (node.op === "<=") out[i] = x <= y ? 1 : 0;
        else if (node.op === "==") out[i] = x === y ? 1 : 0;
        else if (node.op === "!=") out[i] = x !== y ? 1 : 0;
        else out[i] = null;
      }
    }
    return vSeries(out);
  }
  if (node.k === "call") {
    const name = String(node.name || "").toLowerCase();
    if (name === "abs") {
      const x = evalAst(node.args[0], ctx);
      if (isSeriesVal(x)) {
        return vSeries(x.v.map((v) => (Number.isFinite(Number(v)) ? Math.abs(Number(v)) : null)));
      }
      const v = scalarFrom(x);
      return vScalar(v === null ? null : Math.abs(v));
    }

    const ensureSeries = (n) => {
      const v = evalAst(n, ctx);
      if (isSeriesVal(v)) return v.v;
      return [];
    };
    const ensureScalar = (n) => scalarFrom(evalAst(n, ctx));

    if (name === "shift") {
      if (!node.args || node.args.length < 1) return vScalar(null);
      const series = node.args.length >= 2 ? ensureSeries(node.args[0]) : ctx.series.close || [];
      const n = node.args.length >= 2 ? ensureScalar(node.args[1]) : ensureScalar(node.args[0]);
      if (!series.length || n === null) return vScalar(null);
      const k = Math.trunc(n);
      const idx = series.length - 1 - k;
      if (idx < 0 || idx >= series.length) return vScalar(null);
      const v = series[idx];
      return vScalar(v);
    }

    if (["ma", "sma", "ema", "mean", "std"].includes(name)) {
      if (!node.args || node.args.length < 1) return vScalar(null);
      const s = node.args.length >= 2 ? ensureSeries(node.args[0]) : ctx.series.close || [];
      const w = node.args.length >= 2 ? ensureScalar(node.args[1]) : ensureScalar(node.args[0]);
      if (!s.length || w === null) return vScalar(null);
      const win = Math.trunc(w);
      if (name === "std") return vScalar(rollingStd(s, win));
      if (name === "ema") return vScalar(ema(s, win));
      return vScalar(sma(s, win));
    }

    if (name === "rsi") {
      if (!node.args || node.args.length < 1) return vScalar(null);
      const s = node.args.length >= 2 ? ensureSeries(node.args[0]) : ctx.series.close || [];
      const p = node.args.length >= 2 ? ensureScalar(node.args[1]) : ensureScalar(node.args[0]);
      if (!s.length || p === null) return vScalar(null);
      return vScalar(rsi(s, Math.trunc(p)));
    }

    if (name === "corr") {
      if (!node.args) return vScalar(null);
      if (node.args.length >= 3) {
        const x = ensureSeries(node.args[0]);
        const y = ensureSeries(node.args[1]);
        const w = ensureScalar(node.args[2]);
        if (!x.length || !y.length || w === null) return vScalar(null);
        return vScalar(rollingCorr(x, y, Math.trunc(w)));
      }
      if (node.args.length === 2) {
        const a0 = node.args[0];
        const b0 = node.args[1];
        if (a0 && a0.k === "call" && String(a0.name || "").toLowerCase() === "rolling" && Array.isArray(a0.args) && a0.args.length >= 2) {
          const x = ensureSeries(a0.args[0]);
          const w = ensureScalar(a0.args[1]);
          const y = ensureSeries(b0);
          if (!x.length || !y.length || w === null) return vScalar(null);
          return vScalar(rollingCorr(x, y, Math.trunc(w)));
        }
      }
      return vScalar(null);
    }

    return vScalar(null);
  }
  return vScalar(null);
}

function evalExpression(expr, row) {
  const tokens = tokenizeExpr(expr);
  const ast = parseExprTokens(tokens);
  const series = {
    open: getSeries(row, "open"),
    high: getSeries(row, "high"),
    low: getSeries(row, "low"),
    close: getSeries(row, "close"),
    volume: getSeries(row, "volume"),
    quote_volume: getSeries(row, "quote_volume"),
  };
  const latest = {
    open: lastNonNull(series.open),
    high: lastNonNull(series.high),
    low: lastNonNull(series.low),
    close: lastNonNull(series.close) ?? (row && row.close !== undefined ? Number(row.close) : null),
    volume: lastNonNull(series.volume),
    quote_volume: lastNonNull(series.quote_volume),
  };
  const v = evalAst(ast, { series, latest });
  return scalarFrom(v);
}

function detectTemplateParamCount(template) {
  const s = String(template || "");
  const re = /\bn(\d+)\b/gi;
  let m;
  let max = 0;
  while ((m = re.exec(s))) {
    const n = Number(m[1]);
    if (Number.isFinite(n) && n > max) max = n;
  }
  return max;
}

function expandTemplate(template, params) {
  let s = String(template || "");
  const ps = Array.isArray(params) ? params : [];
  for (let i = 1; i <= 12; i++) {
    const v = ps[i - 1];
    if (v === null || v === undefined || !Number.isFinite(Number(v))) continue;
    const re = new RegExp(`\\bn${i}\\b`, "g");
    s = s.replace(re, String(Number(v)));
  }
  return s;
}

function compare(v, cmp, thr) {
  if (v === null || v === undefined || !Number.isFinite(Number(v))) return false;
  const x = Number(v);
  const t = Number(thr);
  if (!Number.isFinite(t)) return false;
  if (cmp === ">") return x > t;
  if (cmp === ">=") return x >= t;
  if (cmp === "<") return x < t;
  if (cmp === "<=") return x <= t;
  return false;
}

function marketLabel(m) {
  if (m === "spot") return "现货";
  if (m === "swap") return "合约";
  return m || "";
}

function stripQuote(symbol) {
  const s = String(symbol || "");
  if (s.endsWith("-USDT")) return s.slice(0, -5);
  if (s.endsWith("USDT")) return s.slice(0, -4);
  return s;
}

function buildDisplayFields(params, customFactors) {
  const fields = [];
  fields.push({ key: "rank", name: "排名", type: "num", get: (r) => r._rank });
  fields.push({ key: "symbol", name: "币种", type: "str", get: (r) => stripQuote(r.symbol) });
  fields.push({ key: "market", name: "市场", type: "str", get: (r) => marketLabel(r.market) });
  fields.push({ key: "dt_display", name: "时间", type: "dt", get: (r) => r.dt_display });
  fields.push({ key: "close", name: "收盘价", type: "num", get: (r) => r.close });
  fields.push({ key: "pct_change", name: "涨跌幅(%)", type: "num", get: (r) => r.pct_change });

  const maCloseP = params.maPeriodClose;
  const maFast = params.maFast;
  const maSlow = params.maSlow;
  const rsiP = params.rsiPeriod;

  const maSet = [];
  for (const p of [maCloseP, maFast, maSlow]) {
    if (Number.isFinite(p) && p > 0 && !maSet.includes(p)) maSet.push(p);
  }
  for (const p of maSet) {
    fields.push({ key: `ma_${p}`, name: `MA(${p})`, type: "num", get: (r) => r._builtins[`ma_${p}`] });
  }
  if (Number.isFinite(rsiP) && rsiP > 0) {
    fields.push({ key: `rsi_${rsiP}`, name: `RSI(${rsiP})`, type: "num", get: (r) => r._builtins[`rsi_${rsiP}`] });
  }

  // Dynamic Built-ins based on active filters
  if ($("condEma") && $("condEma").checked) {
    fields.push({ key: "ema", name: `EMA(${params.emaPeriod})`, type: "num", get: (r) => (r && r._builtins && Object.prototype.hasOwnProperty.call(r._builtins, "ema")) ? r._builtins.ema : ema(getSeries(r, "close"), params.emaPeriod) });
  }
  if ($("condBollUp") && $("condBollUp").checked) {
    fields.push({ key: "boll_up", name: "BOLLUP", type: "num", get: (r) => {
      if (r && r._builtins && Object.prototype.hasOwnProperty.call(r._builtins, "boll_up")) return r._builtins.boll_up;
      const closes = getSeries(r, "close");
      const ma = sma(closes, params.bollPeriod);
      const std = rollingStd(closes, params.bollPeriod);
      return (ma !== null && std !== null) ? ma + params.bollStd * std : null;
    }});
  }
  if ($("condBollDown") && $("condBollDown").checked) {
    fields.push({ key: "boll_down", name: "BOLLDOWN", type: "num", get: (r) => {
      if (r && r._builtins && Object.prototype.hasOwnProperty.call(r._builtins, "boll_down")) return r._builtins.boll_down;
      const closes = getSeries(r, "close");
      const ma = sma(closes, params.bollDownPeriod);
      const std = rollingStd(closes, params.bollDownPeriod);
      return (ma !== null && std !== null) ? ma - params.bollDownStd * std : null;
    }});
  }
  if ($("condSuper") && $("condSuper").checked) {
    fields.push({ key: "supertrend", name: "Supertrend", type: "num", get: (r) => (r && r._builtins && Object.prototype.hasOwnProperty.call(r._builtins, "supertrend")) ? r._builtins.supertrend : supertrend(getSeries(r, "high"), getSeries(r, "low"), getSeries(r, "close"), params.superAtrPeriod, params.superMult) });
  }
  if ($("condKdj") && $("condKdj").checked) {
    fields.push({ key: "kdj_k", name: "K", type: "num", get: (r) => (r && r._builtins && Object.prototype.hasOwnProperty.call(r._builtins, "kdj_k")) ? r._builtins.kdj_k : kdj(getSeries(r, "high"), getSeries(r, "low"), getSeries(r, "close"), params.kdjN, params.kdjM1, params.kdjM2).k });
    fields.push({ key: "kdj_d", name: "D", type: "num", get: (r) => (r && r._builtins && Object.prototype.hasOwnProperty.call(r._builtins, "kdj_d")) ? r._builtins.kdj_d : kdj(getSeries(r, "high"), getSeries(r, "low"), getSeries(r, "close"), params.kdjN, params.kdjM1, params.kdjM2).d });
    fields.push({ key: "kdj_j", name: "J", type: "num", get: (r) => (r && r._builtins && Object.prototype.hasOwnProperty.call(r._builtins, "kdj_j")) ? r._builtins.kdj_j : kdj(getSeries(r, "high"), getSeries(r, "low"), getSeries(r, "close"), params.kdjN, params.kdjM1, params.kdjM2).j });
  }
  if ($("condObv") && $("condObv").checked) {
    fields.push({ key: "obv", name: "OBV", type: "num", get: (r) => (r && r._builtins && Object.prototype.hasOwnProperty.call(r._builtins, "obv")) ? r._builtins.obv : obvWithMa(getSeries(r, "close"), getSeries(r, "volume"), params.obvMaPeriod).obv });
    fields.push({ key: "obv_ma", name: "OBV_MA", type: "num", get: (r) => (r && r._builtins && Object.prototype.hasOwnProperty.call(r._builtins, "obv_ma")) ? r._builtins.obv_ma : obvWithMa(getSeries(r, "close"), getSeries(r, "volume"), params.obvMaPeriod).ma });
  }
  if ($("condStochRsi") && $("condStochRsi").checked) {
    fields.push({ key: "stoch_rsi_k", name: "StochK", type: "num", get: (r) => (r && r._builtins && Object.prototype.hasOwnProperty.call(r._builtins, "stoch_rsi_k")) ? r._builtins.stoch_rsi_k : stochRsi(getSeries(r, "close"), params.stochRsiP, params.stochRsiK, params.stochRsiSmK, params.stochRsiSmD).k });
    fields.push({ key: "stoch_rsi_d", name: "StochD", type: "num", get: (r) => (r && r._builtins && Object.prototype.hasOwnProperty.call(r._builtins, "stoch_rsi_d")) ? r._builtins.stoch_rsi_d : stochRsi(getSeries(r, "close"), params.stochRsiP, params.stochRsiK, params.stochRsiSmK, params.stochRsiSmD).d });
  }

  for (const f of customFactors.filter((x) => x.show)) {
    fields.push({ key: `expr_${f.id}`, name: f.name || f.expr, type: "num", get: (r) => (r._expr ? r._expr[f.id] : null) });
  }

  // Filter based on user visibility preference
  const pinned = new Set(["rank", "symbol", "market", "dt_display", "close", "pct_change"]);
  const finalFields = fields.filter(f => {
    if (pinned.has(f.key)) return true;
    if (state.columnVisibility[f.key] === undefined) return true; // Default visible
    return state.columnVisibility[f.key];
  });

  updateColumnSelector(fields);
  savePlotKeys(finalFields.map((x) => x.key));

  return finalFields;
}

function updateColumnSelector(allFields) {
  const host = $("columnSelector");
  if (!host) return;
  
  // Only rebuild if fields changed or first time
  const currentCount = host.querySelectorAll('label').length;
  if (currentCount === allFields.length) return;

  host.innerHTML = "";
  const pinned = new Set(["rank", "symbol", "market", "dt_display", "close", "pct_change"]);
  for (const f of allFields) {
    const label = document.createElement("label");
    const cb = document.createElement("input");
    cb.type = "checkbox";
    if (pinned.has(f.key)) {
      cb.checked = true;
      cb.disabled = true;
    } else {
      cb.checked = state.columnVisibility[f.key] !== false;
      cb.addEventListener("change", () => {
        state.columnVisibility[f.key] = cb.checked;
        saveColumnVisibility();
        rerenderFromLatest();
      });
    }
    label.appendChild(cb);
    label.appendChild(document.createTextNode(f.name || f.key));
    host.appendChild(label);
  }
}

function buildSortOptionsFromFields(fields, current) {
  const sortKeyEl = $("sortKey");
  const sortOrderEl = $("sortOrder");
  const existed = new Set(fields.map((f) => f.key));
  const prevKey = sortKeyEl.value || current?.key;

  sortKeyEl.innerHTML = "";
  for (const f of fields) {
    const opt = document.createElement("option");
    opt.value = f.key;
    opt.textContent = f.name || f.key;
    sortKeyEl.appendChild(opt);
  }

  const defKey = String((state.meta && state.meta.default_sort && state.meta.default_sort.key) || "pct_change");
  const nextKey = existed.has(prevKey) ? prevKey : (existed.has(defKey) ? defKey : (fields.find((f) => f.key.startsWith("rsi_"))?.key || "close"));
  sortKeyEl.value = nextKey;
  sortOrderEl.value = sortOrderEl.value || current?.order || String((state.meta && state.meta.default_sort && state.meta.default_sort.order) || "desc");
}

function buildTableHeader(fields) {
  const thead = $("thead");
  thead.innerHTML = "";
  const tr = document.createElement("tr");
  for (const f of fields) {
    const th = document.createElement("th");
    if (f.key === "symbol") {
      const wrap = document.createElement("div");
      wrap.className = "th-symbol";
      const label = document.createElement("span");
      label.textContent = f.name || f.key;
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "th-icon-btn";
      btn.setAttribute("aria-label", "搜索币种");
      btn.innerHTML = '<svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2"><path d="M6 2h12M6 22h12"/><path d="M8 2v6l4 4 4-4V2"/><path d="M8 22v-6l4-4 4 4v6"/></svg>';
      const inp = document.createElement("input");
      inp.id = "symbolFilter";
      inp.className = "input th-filter hidden";
      inp.type = "text";
      inp.placeholder = "搜币种…";
      inp.value = String(state.symbolQuery || "");
      btn.addEventListener("click", () => {
        inp.classList.toggle("hidden");
        if (!inp.classList.contains("hidden")) inp.focus();
      });
      inp.addEventListener("blur", () => {
        state.symbolQuery = inp.value || "";
        rerenderFromLatest();
      });
      inp.addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
          e.preventDefault();
          state.symbolQuery = inp.value || "";
          rerenderFromLatest();
          inp.blur();
          return;
        }
        if (e.key === "Escape") {
          inp.value = "";
          state.symbolQuery = "";
          inp.classList.add("hidden");
          rerenderFromLatest();
        }
      });
      wrap.appendChild(label);
      wrap.appendChild(btn);
      wrap.appendChild(inp);
      th.appendChild(wrap);
    } else {
      th.textContent = f.name || f.key;
    }
    if (f.type === "num") th.className = "num";
    tr.appendChild(th);
  }
  thead.appendChild(tr);
}

function renderTable(rows, fields) {
  const tbody = $("tbody");
  tbody.innerHTML = "";
  for (const r of rows) {
    const tr = document.createElement("tr");
    const rk = rowKey(r);
    tr.setAttribute("data-rk", rk);
    tr.addEventListener("click", () => {
      state.selectedKey = rk;
      state.selectedRow = r;
      try {
        localStorage.setItem(selectedKeyStorageKey, rk);
      } catch {}
      renderKline(r);
      ensureKlineSeriesForRow(r);
      applySelectedToTable();
    });
    for (const f of fields) {
      const td = document.createElement("td");
      const raw = f.get ? f.get(r) : r[f.key];
      if (f.key === "symbol") {
        const btn = document.createElement("button");
        btn.className = "coin-link";
        btn.type = "button";
        btn.textContent = String(raw === null || raw === undefined ? "" : raw);
        btn.addEventListener("click", (e) => {
          e.stopPropagation();
          try {
            localStorage.setItem(selectedKeyStorageKey, rk);
          } catch {}
          const u = `./kline.html?key=${encodeURIComponent(rk)}`;
          window.location.href = u;
        });
        td.appendChild(btn);
      } else if (f.type === "dt") td.textContent = fmtDt(raw);
      else if (f.type === "num") {
        td.className = "num";
        if (f.key === "rank") {
          const v = Math.trunc(Number(raw));
          td.textContent = Number.isFinite(v) ? String(v) : "";
        } else td.textContent = fmtNum(raw);
      } else td.textContent = raw === null || raw === undefined ? "" : String(raw);
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
}

function updateSummary(summary) {
  const gen = fmtDt(summary.generated_at);
  let lat = "";
  const latClose = summary.latest_dt_close;
  const latDisp = summary.latest_dt_display;
  if (latClose) {
    lat = fmtDt(latClose);
  } else if (latDisp) {
    const bh = Number((state.latest && state.latest.config && state.latest.config.bar_hours) || 1) || 1;
    const ms0 = Date.parse(String(latDisp));
    if (Number.isFinite(ms0)) lat = fmtDt(ms0 + bh * 3600 * 1000);
    else lat = fmtDt(latDisp);
  }
  $("summary").textContent = `生成：${gen} ｜ 最新：${lat}`;
}

function getParams() {
  const market = getSelectedMarket();
  const symbolQuery = String(state.symbolQuery || "").trim();
  const maPeriodClose = parseIntInput($("maPeriodClose"));
  const maFast = parseIntInput($("maFast"));
  const maSlow = parseIntInput($("maSlow"));
  const rsiPeriod = parseIntInput($("rsiPeriod"));
  const rsiThreshold = parseFloatInput($("rsiThreshold"));

  // New Trend params
  const emaPeriod = parseIntInput($("emaPeriod"));
  const bollPeriod = parseIntInput($("bollPeriod"));
  const bollStd = parseFloatInput($("bollStd"));
  const bollDownPeriod = parseIntInput($("bollDownPeriod"));
  const bollDownStd = parseFloatInput($("bollDownStd"));
  const superAtrPeriod = parseIntInput($("superAtrPeriod"));
  const superMult = parseFloatInput($("superMult"));

  // New Momentum params
  const kdjN = parseIntInput($("kdjN"));
  const kdjM1 = parseIntInput($("kdjM1"));
  const kdjM2 = parseIntInput($("kdjM2"));
  const obvMaPeriod = parseIntInput($("obvMaPeriod"));
  const stochRsiP = parseIntInput($("stochRsiP"));
  const stochRsiK = parseIntInput($("stochRsiK"));
  const stochRsiSmK = parseIntInput($("stochRsiSmK"));
  const stochRsiSmD = parseIntInput($("stochRsiSmD"));

  return {
    market, symbolQuery, maPeriodClose, maFast, maSlow, rsiPeriod, rsiThreshold,
    emaPeriod, bollPeriod, bollStd, bollDownPeriod, bollDownStd, superAtrPeriod, superMult,
    kdjN, kdjM1, kdjM2, obvMaPeriod, stochRsiP, stochRsiK, stochRsiSmK, stochRsiSmD
  };
}

function computeBuiltins(row, params) {
  const closes = getSeries(row, "close");
  const base = (row && row._builtins && typeof row._builtins === "object") ? row._builtins : {};
  const out = { ...base };
  const ps = [params.maPeriodClose, params.maFast, params.maSlow];
  for (const p of ps) {
    if (!Number.isFinite(p) || p <= 0) continue;
    const k = `ma_${p}`;
    if (row && row._builtins && Object.prototype.hasOwnProperty.call(row._builtins, k)) out[k] = row._builtins[k];
    else out[k] = sma(closes, p);
  }
  if (Number.isFinite(params.rsiPeriod) && params.rsiPeriod > 0) {
    const k = `rsi_${params.rsiPeriod}`;
    if (row && row._builtins && Object.prototype.hasOwnProperty.call(row._builtins, k)) out[k] = row._builtins[k];
    else out[k] = rsi(closes, params.rsiPeriod);
  }
  return out;
}

function applyAllFilters(rows, params, customFactors) {
  let hasSeries = false;
  for (const r of rows || []) {
    if (r && r.series) { hasSeries = true; break; }
  }
  const enabledCloseMa = $("condCloseMa").checked;
  const enabledMa = $("condMa").checked;
  const enabledRsi = $("condRsi").checked;

  const enabledEma = $("condEma") && $("condEma").checked;
  const enabledBollUp = $("condBollUp") && $("condBollUp").checked;
  const enabledBollDown = $("condBollDown") && $("condBollDown").checked;
  const enabledSuper = $("condSuper") && $("condSuper").checked;
  const enabledKdj = $("condKdj") && $("condKdj").checked;
  const enabledObv = $("condObv") && $("condObv").checked;
  const enabledStochRsi = $("condStochRsi") && $("condStochRsi").checked;

  const lists0 = state.baseConfig || {};
  const wl = Array.isArray(lists0.whitelist) ? lists0.whitelist : [];
  const bl = Array.isArray(lists0.blacklist) ? lists0.blacklist : [];
  const whitelist = new Set(wl.map((x) => String(x || "").trim().toUpperCase()).filter((x) => x));
  const blacklist = new Set(bl.map((x) => String(x || "").trim().toUpperCase()).filter((x) => x));
  const baseSymbol = (sym) => {
    const s = String(sym || "").trim().toUpperCase();
    if (s.endsWith("-USDT")) return s.slice(0, -5);
    if (s.endsWith("USDT")) return s.slice(0, -4);
    if (s.includes("-")) return s.split("-", 1)[0];
    return s;
  };

  const selected = [];
  let filteredOut = 0;
  let exprErrors = 0;
  let exprMissing = 0;
  let missingBuiltins = 0;

  for (const r of rows) {
    if (params.market !== "all" && String(r.market) !== String(params.market)) {
      continue;
    }
    if (params.symbolQuery) {
      const q = String(params.symbolQuery || "").toUpperCase();
      const s = String(r.symbol || "").toUpperCase();
      if (!s.includes(q)) continue;
    }
    const sym0 = String(r.symbol || "").toUpperCase();
    const bs0 = baseSymbol(sym0);
    if (whitelist.size && !whitelist.has(sym0) && !whitelist.has(bs0)) continue;
    if (blacklist.size && (blacklist.has(sym0) || blacklist.has(bs0))) continue;
    r._builtins = computeBuiltins(r, params);
    r._expr = (r && r._expr && typeof r._expr === "object") ? r._expr : {};

    const closes = getSeries(r, "close");
    const highs = getSeries(r, "high");
    const lows = getSeries(r, "low");
    const volumes = getSeries(r, "volume");
    const lastClose = Number(r.close);

    if (enabledCloseMa) {
      const k = `ma_${params.maPeriodClose}`;
      const maV = r._builtins[k];
      if (maV === null || maV === undefined || !Number.isFinite(Number(maV))) {
        missingBuiltins++;
      } else if (!(lastClose > Number(maV))) {
        filteredOut++; continue;
      }
    }

    if (enabledMa) {
      const kf = `ma_${params.maFast}`;
      const ks = `ma_${params.maSlow}`;
      const maF = r._builtins[kf];
      const maS = r._builtins[ks];
      if (maF === null || maS === null || maF === undefined || maS === undefined || !Number.isFinite(Number(maF)) || !Number.isFinite(Number(maS))) {
        missingBuiltins++;
      } else if (!(Number(maF) > Number(maS))) {
        filteredOut++; continue;
      }
    }

    if (enabledRsi) {
      const kr = `rsi_${params.rsiPeriod}`;
      const rv = r._builtins[kr];
      if (rv === null || rv === undefined || !Number.isFinite(Number(rv))) {
        missingBuiltins++;
      } else if (!(Number(rv) > Number(params.rsiThreshold))) {
        filteredOut++; continue;
      }
    }

    if (enabledEma) {
      const ev = (r._builtins && Object.prototype.hasOwnProperty.call(r._builtins, "ema")) ? r._builtins.ema : (hasSeries ? ema(closes, params.emaPeriod) : null);
      if (ev === null || ev === undefined || !Number.isFinite(Number(ev))) missingBuiltins++;
      else if (!(lastClose > Number(ev))) { filteredOut++; continue; }
    }

    if (enabledBollUp) {
      const bv = (r._builtins && Object.prototype.hasOwnProperty.call(r._builtins, "boll_up")) ? r._builtins.boll_up : null;
      if (bv === null || bv === undefined || !Number.isFinite(Number(bv))) {
        if (hasSeries) {
          const ma = sma(closes, params.bollPeriod);
          const std = rollingStd(closes, params.bollPeriod);
          if (ma === null || std === null) missingBuiltins++;
          else if (!(lastClose > (ma + params.bollStd * std))) { filteredOut++; continue; }
        } else missingBuiltins++;
      } else if (!(lastClose > Number(bv))) { filteredOut++; continue; }
    }

    if (enabledBollDown) {
      const bv = (r._builtins && Object.prototype.hasOwnProperty.call(r._builtins, "boll_down")) ? r._builtins.boll_down : null;
      if (bv === null || bv === undefined || !Number.isFinite(Number(bv))) {
        if (hasSeries) {
          const ma = sma(closes, params.bollDownPeriod);
          const std = rollingStd(closes, params.bollDownPeriod);
          if (ma === null || std === null) missingBuiltins++;
          else if (!(lastClose < (ma - params.bollDownStd * std))) { filteredOut++; continue; }
        } else missingBuiltins++;
      } else if (!(lastClose < Number(bv))) { filteredOut++; continue; }
    }

    if (enabledSuper) {
      const stv = (r._builtins && Object.prototype.hasOwnProperty.call(r._builtins, "supertrend")) ? r._builtins.supertrend : (hasSeries ? supertrend(highs, lows, closes, params.superAtrPeriod, params.superMult) : null);
      if (stv === null || stv === undefined || !Number.isFinite(Number(stv))) missingBuiltins++;
      else if (!(lastClose > Number(stv))) { filteredOut++; continue; }
    }

    if (enabledKdj) {
      const kv = r._builtins && Object.prototype.hasOwnProperty.call(r._builtins, "kdj_k") ? r._builtins.kdj_k : null;
      const dv = r._builtins && Object.prototype.hasOwnProperty.call(r._builtins, "kdj_d") ? r._builtins.kdj_d : null;
      if (kv === null || dv === null || kv === undefined || dv === undefined || !Number.isFinite(Number(kv)) || !Number.isFinite(Number(dv))) {
        if (hasSeries) {
          const { k, d } = kdj(highs, lows, closes, params.kdjN, params.kdjM1, params.kdjM2);
          if (k === null || d === null) missingBuiltins++;
          else if (!(k > d)) { filteredOut++; continue; }
        } else missingBuiltins++;
      } else if (!(Number(kv) > Number(dv))) { filteredOut++; continue; }
    }

    if (enabledObv) {
      const ov = r._builtins && Object.prototype.hasOwnProperty.call(r._builtins, "obv") ? r._builtins.obv : null;
      const om = r._builtins && Object.prototype.hasOwnProperty.call(r._builtins, "obv_ma") ? r._builtins.obv_ma : null;
      if (ov === null || om === null || ov === undefined || om === undefined || !Number.isFinite(Number(ov)) || !Number.isFinite(Number(om))) {
        if (hasSeries) {
          const x = obvWithMa(closes, volumes, params.obvMaPeriod);
          if (x.obv === null || x.ma === null) missingBuiltins++;
          else if (!(x.obv > x.ma)) { filteredOut++; continue; }
        } else missingBuiltins++;
      } else if (!(Number(ov) > Number(om))) { filteredOut++; continue; }
    }

    if (enabledStochRsi) {
      const kv = r._builtins && Object.prototype.hasOwnProperty.call(r._builtins, "stoch_rsi_k") ? r._builtins.stoch_rsi_k : null;
      const dv = r._builtins && Object.prototype.hasOwnProperty.call(r._builtins, "stoch_rsi_d") ? r._builtins.stoch_rsi_d : null;
      if (kv === null || dv === null || kv === undefined || dv === undefined || !Number.isFinite(Number(kv)) || !Number.isFinite(Number(dv))) {
        if (hasSeries) {
          const x = stochRsi(closes, params.stochRsiP, params.stochRsiK, params.stochRsiSmK, params.stochRsiSmD);
          if (x.k === null || x.d === null) missingBuiltins++;
          else if (!(x.k > x.d)) { filteredOut++; continue; }
        } else missingBuiltins++;
      } else if (!(Number(kv) > Number(dv))) { filteredOut++; continue; }
    }

    let exprFail = false;
    for (const f of customFactors) {
      try {
        let v = null;
        if (hasSeries) {
          const template = f.template || f.expr || "";
          const expr = expandTemplate(template, f.params || []);
          v = evalExpression(expr, r);
          r._expr[f.id] = v;
        } else {
          v = r._expr ? r._expr[f.id] : null;
        }
        if (!f.enabled) continue;
        if (v === null || v === undefined || !Number.isFinite(Number(v))) {
          exprMissing++;
          continue;
        }
        if (f.thresholdEnabled) {
          if (!compare(v, f.cmp, f.threshold)) {
            exprFail = true;
            break;
          }
        } else {
          if (Number(v) === 0) {
            exprFail = true;
            break;
          }
        }
      } catch {
        exprErrors++;
        continue;
      }
    }
    if (exprFail) {
      filteredOut++;
      continue;
    }

    selected.push(r);
  }

  return { selected, filteredOut, exprErrors, exprMissing, missingBuiltins, hasSeries };
}

function sortRows(rows, fields) {
  const key = $("sortKey").value;
  const order = $("sortOrder").value;
  const dir = order === "asc" ? 1 : -1;
  const field = fields.find((f) => f.key === key);
  const getter = field && field.get ? field.get : (r) => r[key];

  const sorted = [...rows].sort((a, b) => {
    const av = getter(a);
    const bv = getter(b);
    if (av === null || av === undefined) return 1;
    if (bv === null || bv === undefined) return -1;
    if (typeof av === "string" || typeof bv === "string") return String(av).localeCompare(String(bv)) * dir;
    return (Number(av) - Number(bv)) * dir;
  });
  return { sorted, sortKey: key };
}

function assignRank(rows, sortKey, fields) {
  const field = fields.find((f) => f.key === sortKey);
  const getter = field && field.get ? field.get : (r) => r[sortKey];
  const numeric = rows.filter((r) => getter(r) !== null && getter(r) !== undefined && Number.isFinite(Number(getter(r))));
  const set = new Set(numeric);
  let i = 1;
  for (const r of rows) {
    if (set.has(r)) r._rank = i++;
    else r._rank = "";
  }
  return rows;
}

function renderCustomFactorList() {
  const el = $("exprList");
  el.innerHTML = "";
  const items = state.customFactors;
  if (!items.length) {
    el.textContent = "暂无";
    return;
  }
  for (const f of items) {
    const row = document.createElement("div");
    row.style.display = "flex";
    row.style.gap = "8px";
    row.style.alignItems = "center";

    const title = document.createElement("div");
    title.style.flex = "1 1 auto";
    title.textContent = `[${f.folder}] ${f.name}`;
    row.appendChild(title);

    const cb1 = document.createElement("input");
    cb1.type = "checkbox";
    cb1.checked = !!f.enabled;
    cb1.addEventListener("change", () => {
      f.enabled = cb1.checked;
      saveCustomFactors(state.customFactors);
      syncCustomFactorsToServer(state.customFactors);
      renderFolderConditions();
      if (cb1.checked) refreshFactors();
      else rerenderFromLatest();
    });
    row.appendChild(cb1);

    const cb2 = document.createElement("input");
    cb2.type = "checkbox";
    cb2.checked = !!f.show;
    cb2.addEventListener("change", () => {
      f.show = cb2.checked;
      saveCustomFactors(state.customFactors);
      syncCustomFactorsToServer(state.customFactors);
      if (cb2.checked) refreshFactors();
      else rerenderFromLatest();
    });
    row.appendChild(cb2);

    const del = document.createElement("button");
    del.textContent = "删";
    del.className = "btn btn-secondary";
    del.style.width = "52px";
    del.style.padding = "6px 8px";
    del.addEventListener("click", () => {
      state.customFactors = state.customFactors.filter((x) => x.id !== f.id);
      saveCustomFactors(state.customFactors);
      syncCustomFactorsToServer(state.customFactors);
      renderFolderConditions();
      renderCustomFactorList();
      rerenderFromLatest();
    });
    row.appendChild(del);

    el.appendChild(row);
  }
}

function getBuiltInFolderBaseCount(folder) {
  if (folder === "均线趋势") return 2;
  if (folder === "动量") return 1;
  return 0;
}

function getFolderTitle(folder) {
  if (folder === "均线趋势") return "均线趋势";
  if (folder === "动量") return "动量";
  return folder || "默认";
}

function buildFactorLabel(folder, idx, f) {
  const left = f.name || "factor";
  const cmp = f.cmp || ">=";
  const thr = f.threshold;
  if (f.thresholdEnabled) return `条件${idx}：${left}${cmp}${thr}`;
  return `条件${idx}：${left}`;
}

function renderFolderConditions() {
  const byFolder = new Map();
  for (const f of state.customFactors) {
    const folder = f.folder || "默认";
    if (!byFolder.has(folder)) byFolder.set(folder, []);
    byFolder.get(folder).push(f);
  }

  const mount = (folder, container) => {
    container.innerHTML = "";
    const items = byFolder.get(folder) || [];
    const base = getBuiltInFolderBaseCount(folder);
    for (let i = 0; i < items.length; i++) {
      const f = items[i];
      const line = document.createElement("div");
      line.className = "cond-line";

      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.checked = !!f.enabled;
      cb.addEventListener("change", () => {
        f.enabled = cb.checked;
        saveCustomFactors(state.customFactors);
        syncCustomFactorsToServer(state.customFactors);
        renderFolderConditions();
        if (cb.checked) refreshFactors();
        else rerenderFromLatest();
      });
      line.appendChild(cb);

      const text = document.createElement("div");
      text.style.flex = "1 1 auto";
      text.textContent = buildFactorLabel(folder, base + i + 1, f);
      line.appendChild(text);

      const pcount = Array.isArray(f.params) ? f.params.length : 0;
      if (pcount > 0) {
        for (let j = 0; j < pcount; j++) {
          const inp = document.createElement("input");
          inp.className = "input mini";
          inp.type = "number";
          inp.step = "1";
          inp.value = String(f.params[j]);
          inp.title = `n${j + 1}`;
          inp.addEventListener("change", () => {
            const v = Number(inp.value);
            if (!Number.isFinite(v)) return;
            f.params[j] = v;
            saveCustomFactors(state.customFactors);
            syncCustomFactorsToServer(state.customFactors);
            refreshFactors();
          });
          line.appendChild(inp);
        }
      }

      const del = document.createElement("button");
      del.textContent = "删";
      del.className = "btn btn-secondary mini";
      del.addEventListener("click", () => {
        state.customFactors = state.customFactors.filter((x) => x.id !== f.id);
        saveCustomFactors(state.customFactors);
        syncCustomFactorsToServer(state.customFactors);
        renderFolderConditions();
        renderCustomFactorList();
        rerenderFromLatest();
      });
      line.appendChild(del);

      container.appendChild(line);
    }
  };

  const maEl = $("folder_ma_trend");
  const momEl = $("folder_momentum");
  if (maEl) mount("均线趋势", maEl);
  if (momEl) mount("动量", momEl);

  const other = [];
  for (const [folder] of byFolder.entries()) {
    if (folder !== "均线趋势" && folder !== "动量") other.push(folder);
  }

  const host = $("customFolders");
  if (host) {
    host.innerHTML = "";
    for (const folder of other.sort()) {
      const block = document.createElement("div");
      block.className = "block";

      const title = document.createElement("div");
      title.className = "block-title";
      title.textContent = getFolderTitle(folder);
      block.appendChild(title);

      const list = document.createElement("div");
      list.className = "folder-conds";
      block.appendChild(list);

      host.appendChild(block);
      mount(folder, list);
    }
  }
}

function updateFolderOptions() {
  const dl = $("folderOptions");
  if (!dl) return;
  dl.innerHTML = "";
  const base = ["均线趋势", "动量"];
  const folders = new Set(base);
  for (const f of state.customFactors) folders.add(f.folder || "默认");
  for (const folder of Array.from(folders)) {
    const opt = document.createElement("option");
    opt.value = folder;
    dl.appendChild(opt);
  }
}

function setExprEditorOpen(open) {
  state.exprEditorOpen = !!open;
  const editor = $("exprEditor");
  const list = $("exprList");
  if (editor) editor.classList.toggle("hidden", !state.exprEditorOpen);
  if (list) list.classList.toggle("hidden", !state.exprEditorOpen);
  const btn = $("exprToggle");
  if (btn) btn.textContent = state.exprEditorOpen ? "收起" : "添加因子";

  if (state.exprEditorOpen) {
    // Generate a default name like factor1, factor2...
    const names = new Set(state.customFactors.map((f) => f.name));
    let idx = 1;
    while (names.has(`factor${idx}`)) idx++;
    $("exprName").value = `factor${idx}`;
    $("exprFolder").value = "";
  }
}

function syncExprThresholdUI() {
  const enabled = $("exprEnable") && $("exprEnable").checked;
  const row = $("exprThresholdRow");
  if (row) row.classList.toggle("hidden", !enabled);
}

function setHelpOpen(open) {
  const modal = $("helpModal");
  if (!modal) return;
  modal.classList.toggle("hidden", !open);
}

function setModalOpen(id, open) {
  const modal = $(id);
  if (!modal) return;
  modal.classList.toggle("hidden", !open);
}

function setText(id, text) {
  const el = $(id);
  if (!el) return;
  el.textContent = text || "";
}

function closeProfileMenu() {
  const menu = $("profileMenu");
  if (menu) menu.classList.add("hidden");
  const sub = $("helpSubmenu");
  if (sub) sub.classList.add("hidden");
  const helpBtn = $("menuHelp");
  if (helpBtn) helpBtn.setAttribute("aria-expanded", "false");
}

async function loadMe() {
  try {
    const r = await fetch("./api/me", { cache: "no-store" });
    if (!r.ok) return null;
    const j = await r.json();
    return j && j.user ? j.user : null;
  } catch {
    return null;
  }
}

async function loadWecomConfig() {
  try {
    const r = await fetch("./api/wecom_config", { cache: "no-store" });
    if (!r.ok) return null;
    const j = await r.json();
    return j && j.config ? j.config : null;
  } catch {
    return null;
  }
}

function formatEmailForAvatar(email, username) {
  const s = String(email || username || "U").trim();
  if (!s) return "U";
  return s[0].toUpperCase();
}

function buildWecomConfigFromCurrent() {
  const params = getParams();
  const toggles = {
    condCloseMa: !!($("condCloseMa") && $("condCloseMa").checked),
    condMa: !!($("condMa") && $("condMa").checked),
    condRsi: !!($("condRsi") && $("condRsi").checked),
    condEma: !!($("condEma") && $("condEma").checked),
    condBollUp: !!($("condBollUp") && $("condBollUp").checked),
    condBollDown: !!($("condBollDown") && $("condBollDown").checked),
    condSuper: !!($("condSuper") && $("condSuper").checked),
    condKdj: !!($("condKdj") && $("condKdj").checked),
    condObv: !!($("condObv") && $("condObv").checked),
    condStochRsi: !!($("condStochRsi") && $("condStochRsi").checked),
  };
  const customFactors = (state.customFactors || []).map((f) => ({
    id: f.id,
    folder: f.folder,
    name: f.name,
    template: f.template || f.expr || "",
    params: Array.isArray(f.params) ? f.params : [],
    enabled: !!f.enabled,
    thresholdEnabled: !!f.thresholdEnabled,
    cmp: f.cmp || ">=",
    threshold: Number(f.threshold),
  }));
  const defKey = String((state.meta && state.meta.default_sort && state.meta.default_sort.key) || "pct_change");
  const defOrder = String((state.meta && state.meta.default_sort && state.meta.default_sort.order) || "desc");
  const sortKey = ($("sortKey") && $("sortKey").value) || defKey;
  const sortOrder = ($("sortOrder") && $("sortOrder").value) || defOrder;
  const lists = state.baseConfig || loadBaseConfig();
  return { params, toggles, customFactors, sort: { key: sortKey, order: sortOrder }, lists };
}

function updateWecomSummary(cfg) {
  const el = $("wecomSummary");
  if (!el) return;
  if (!cfg) {
    el.textContent = "-";
    return;
  }
  const t = cfg.toggles || {};
  const on = [];
  for (const k of ["condCloseMa", "condMa", "condEma", "condBollUp", "condBollDown", "condSuper", "condRsi", "condKdj", "condObv", "condStochRsi"]) {
    if (t[k]) on.push(k);
  }
  const custom = Array.isArray(cfg.customFactors) ? cfg.customFactors.filter((x) => x && x.enabled) : [];
  const lines = [];
  const mk0 = cfg.params && cfg.params.market ? String(cfg.params.market) : "all";
  const mk = mk0 === "spot" || mk0 === "swap" || mk0 === "all" ? mk0 : "all";
  const mkLabel = mk === "spot" ? "现货" : (mk === "swap" ? "合约" : "全市场");
  lines.push(`市场：${mkLabel}`);
  lines.push(`内置条件：${on.length ? on.join(", ") : "无"}`);
  lines.push(`自定义条件：${custom.length}`);
  lines.push(`排序：${cfg.sort && cfg.sort.key ? cfg.sort.key : "close"} ${cfg.sort && cfg.sort.order ? cfg.sort.order : "desc"}`);
  el.textContent = lines.join(" ｜ ");
}

function parseSymbolListText(text) {
  const s = String(text || "").trim();
  if (!s) return [];
  const parts = s.split(/[\s,，;；]+/g).map((x) => String(x || "").trim().toUpperCase()).filter((x) => x);
  const uniq = [];
  const seen = new Set();
  for (const x of parts) {
    if (seen.has(x)) continue;
    seen.add(x);
    uniq.push(x);
  }
  return uniq;
}

function renderStrategyGroupSelect() {
  const sel = $("strategySelect");
  if (!sel) return;
  sel.innerHTML = "";
  const gs = Array.isArray(state.strategyGroups) ? state.strategyGroups : [];
  if (!gs.length) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "（无策略组）";
    sel.appendChild(opt);
    return;
  }
  const opt0 = document.createElement("option");
  opt0.value = "";
  opt0.textContent = "（不选择）";
  sel.appendChild(opt0);
  for (const g of gs) {
    const opt = document.createElement("option");
    opt.value = String(g.id || "");
    opt.textContent = String(g.name || g.id || "");
    sel.appendChild(opt);
  }
}

function getSelectedStrategyGroup() {
  const sel = $("strategySelect");
  const id = sel ? String(sel.value || "") : "";
  if (!id) return null;
  const gs = Array.isArray(state.strategyGroups) ? state.strategyGroups : [];
  return gs.find((x) => String(x.id || "") === id) || null;
}

function mergeConfigWithBase(cfg) {
  const base = state.baseConfig || loadBaseConfig();
  const out = Object.assign({}, cfg || {});
  out.lists = { whitelist: (base.whitelist || []), blacklist: (base.blacklist || []) };
  return out;
}

function applyConfigToUI(cfg) {
  const params = (cfg && cfg.params) ? cfg.params : {};
  const toggles = (cfg && cfg.toggles) ? cfg.toggles : {};
  if (params.market) setSelectedMarket(params.market);
  for (const [id, v] of Object.entries(params)) {
    const el = $(id);
    if (!el) continue;
    if (el.tagName === "INPUT" || el.tagName === "SELECT" || el.tagName === "TEXTAREA") {
      if (el.type === "checkbox") el.checked = !!v;
      else el.value = String(v);
    }
  }
  for (const [k, v] of Object.entries(toggles)) {
    const el = $(k);
    if (el && el.type === "checkbox") el.checked = !!v;
  }
  if ($("sortKey") && cfg && cfg.sort && cfg.sort.key) $("sortKey").value = String(cfg.sort.key);
  if ($("sortOrder") && cfg && cfg.sort && cfg.sort.order) $("sortOrder").value = String(cfg.sort.order);
  if (Array.isArray(cfg && cfg.customFactors)) {
    state.customFactors = cfg.customFactors.map((f) => ({
      id: f.id,
      folder: f.folder || "默认",
      name: f.name,
      template: f.template || "",
      params: Array.isArray(f.params) ? f.params : [],
      enabled: !!f.enabled,
      thresholdEnabled: !!f.thresholdEnabled,
      cmp: f.cmp || ">=",
      threshold: Number(f.threshold),
      showColumn: f.showColumn !== undefined ? !!f.showColumn : (f.show !== undefined ? !!f.show : true),
      show: f.show !== undefined ? !!f.show : (f.showColumn !== undefined ? !!f.showColumn : true),
    }));
    saveCustomFactors(state.customFactors);
    updateFolderOptions();
    renderFolderConditions();
    renderCustomFactorList();
  }
}

async function openBaseConfigModal() {
  renderStrategyGroupSelect();
  const cfg = state.baseConfig || loadBaseConfig();
  state.baseConfig = cfg;
  if ($("whitelistText")) $("whitelistText").value = (cfg.whitelist || []).join(",");
  if ($("blacklistText")) $("blacklistText").value = (cfg.blacklist || []).join(",");
  if ($("baseMarketSelect")) $("baseMarketSelect").value = getSelectedMarket();
  if ($("sendEmailTo")) {
    try {
      const raw = localStorage.getItem("crypto_screener_send_email_to_v1");
      if (raw) $("sendEmailTo").value = String(raw || "");
    } catch {}
  }
  const serverCfg = await loadWecomConfig();
  if (serverCfg) {
    if ($("wecomWebhook")) $("wecomWebhook").value = String(serverCfg.webhook_url || "");
    if ($("wecomEnabled")) $("wecomEnabled").checked = !!serverCfg.enabled;
    if ($("wecomTopN")) $("wecomTopN").value = String(serverCfg.top_n || 20);
    if (serverCfg.config) state.strategyDraft = normalizeStrategyConfig(serverCfg.config);
    else state.strategyDraft = normalizeStrategyConfig(buildWecomConfigFromCurrent());
  } else {
    state.strategyDraft = normalizeStrategyConfig(buildWecomConfigFromCurrent());
  }
  if (state.strategyDraft && state.strategyDraft.params && state.strategyDraft.params.market) setSelectedMarket(state.strategyDraft.params.market);
  renderStrategyCondEditor();
  updateWecomSummary(state.strategyDraft);
  setText("wecomResult", "-");
  setBaseCfgPane(loadBaseCfgPane());
  setModalOpen("wecomModal", true);
}

async function postJson(url, data) {
  const r = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(data || {}) });
  if (r.status === 401) {
    const next = encodeURIComponent(location.pathname + location.search);
    location.href = `./login.html?next=${next}`;
    return { r, j: null };
  }
  let j = null;
  try { j = await r.json(); } catch {}
  return { r, j };
}

function renderExprParams(count) {
  const el = $("exprParams");
  if (!el) return;
  el.innerHTML = "";
  if (!count) return;

  const vals = Array.isArray(state.exprDraftParams) ? state.exprDraftParams : [];
  const next = [];
  for (let i = 0; i < count; i++) next.push(Number.isFinite(Number(vals[i])) ? Number(vals[i]) : (i === 0 ? 10 : i + 1));
  state.exprDraftParams = next;

  for (let i = 0; i < count; i++) {
    const item = document.createElement("div");
    item.className = "param-item";

    const tag = document.createElement("span");
    tag.className = "label-inline";
    tag.textContent = `n${i + 1}`;
    item.appendChild(tag);

    const inp = document.createElement("input");
    inp.className = "input";
    inp.type = "number";
    inp.step = "1";
    inp.value = String(next[i]);
    inp.addEventListener("change", () => {
      const v = Number(inp.value);
      if (!Number.isFinite(v)) return;
      state.exprDraftParams[i] = v;
    });
    item.appendChild(inp);

    el.appendChild(item);
  }
}

function upsertCustomFactor() {
  const folder = ($("exprFolder").value || "默认").trim() || "默认";
  const name = ($("exprName").value || "").trim() || "expr";
  let template = ($("exprText").value || "").trim();
  let cmp = $("exprCmp").value || ">=";
  let threshold = Number($("exprThreshold").value);
  let thresholdEnabled = $("exprEnable").checked;
  const show = $("exprShow").checked;

  if (!template) {
    $("counts").textContent = "表达式不能为空";
    return;
  }

  // Auto detect threshold mode if it's a comparison with a number
  try {
    const tokens = tokenizeExpr(template);
    const ast = parseExprTokens(tokens);
    if (ast.k === "cmp") {
      let detected = false;
      let subExpr = "";
      if (ast.b.k === "num") {
        threshold = ast.b.v;
        cmp = ast.op;
        // Get the string representation of ast.a
        // Since we don't have a stringifier, we'll try to slice the original string
        const opIdx = template.lastIndexOf(ast.op);
        if (opIdx !== -1) {
          subExpr = template.slice(0, opIdx).trim();
          detected = true;
        }
      } else if (ast.a.k === "num") {
        threshold = ast.a.v;
        const inverse = { ">": "<", ">=": "<=", "<": ">", "<=": ">=", "==": "==", "!=": "!=" };
        cmp = inverse[ast.op] || ast.op;
        const opIdx = template.indexOf(ast.op);
        if (opIdx !== -1) {
          subExpr = template.slice(opIdx + ast.op.length).trim();
          detected = true;
        }
      }

      if (detected && subExpr) {
        template = subExpr;
        thresholdEnabled = true;
      }
    }
  } catch (e) {
    console.error("Auto-threshold detection failed:", e);
  }

  const pcount = detectTemplateParamCount(template);
  const params = (state.exprDraftParams || []).slice(0, pcount).map((x) => Number(x));

  let exist = state.customFactors.find((x) => x.folder === folder && x.name === name);
  if (!exist) {
    exist = { id: newId(), folder, name, template, params, cmp, threshold, enabled: true, thresholdEnabled, show };
    state.customFactors = [exist, ...state.customFactors];
  } else {
    exist.template = template;
    exist.params = params;
    exist.cmp = cmp;
    exist.threshold = threshold;
    if (exist.enabled === undefined) exist.enabled = true;
    exist.thresholdEnabled = thresholdEnabled;
    exist.show = show;
  }
  saveCustomFactors(state.customFactors);
  syncCustomFactorsToServer(state.customFactors);
  updateFolderOptions();
  renderFolderConditions();
  renderCustomFactorList();
  setExprEditorOpen(false);
  refreshFactors();
}

function rerenderFromLatest() {
  try {
    const latest = state.latest;
    if (!latest) return;
    const params = getParams();
    const customFactors = state.customFactors;
    const displayFields = buildDisplayFields(params, customFactors);
    buildSortOptionsFromFields(displayFields, state.meta && state.meta.default_sort);
    buildTableHeader(displayFields);
    const allRows = Array.isArray(latest.results) ? latest.results : [];
    const { selected, exprErrors, exprMissing, missingBuiltins } = applyAllFilters(allRows, params, customFactors);
    const { sorted, sortKey } = sortRows(selected, displayFields);
    assignRank(sorted, sortKey, displayFields);
    renderTable(sorted, displayFields);
    saveLastPicks(sorted, latest.summary || {});
    syncSelectedRowFrom(sorted);
    const parts = [];
    if (exprErrors) parts.push(`表达式异常：${exprErrors}`);
    if (exprMissing) parts.push(`因子缺数据：${exprMissing}`);
    const msg = parts.length ? ` ｜ ${parts.join(" ｜ ")}` : "";
    const total = params.market === "all" ? allRows.length : allRows.filter((r) => String(r.market) === String(params.market)).length;
    $("counts").textContent = `显示：${sorted.length} / ${total}${msg}`;
    updateSummary(latest.summary || {});
  } catch {}
}

async function refresh(opts = {}) {
  try {
    const now = new Date();
    const hh = String(now.getHours()).padStart(2, "0");
    const mm = String(now.getMinutes()).padStart(2, "0");
    const isAuto = !!opts.auto;
    if (opts.factorCompute) setStatus("计算因子中，请稍等");
    else if (isAuto) setStatus(`自动更新中… ${hh}:${mm}`);
    else if (opts.manual) setStatus(`手动刷新… ${hh}:${mm}`);

    showProgress(8, "更新数据...");
    let backendTriggered = false;
    let backendRunning = false;
    const prevMetaUpdatedAt = state.meta && state.meta.updated_at ? String(state.meta.updated_at) : "";
    if (!opts.skipBackend) {
      const r = await triggerBackendRefresh({ timeoutMs: opts.manual ? 450 : 700, fetchMode: opts.fetchMode === false ? false : true });
      if (r && r.status === 202) backendTriggered = true;
      if (r && r.status === 409) {
        const st = await loadStatus();
        const lockPid = st && st.lock && st.lock.pid ? String(st.lock.pid) : "";
        const hint = lockPid ? `（更新进程 PID=${lockPid}）` : "（后台忙）";
        if (opts.manual) $("counts").textContent = `后台正在更新，已改为仅重新读取快照 ${hint}`;
        backendTriggered = false;
        backendRunning = !!(st && st.update && st.update.running);
      } else if (r && !r.ok) {
        backendTriggered = false;
      } else if (!r) {
        backendTriggered = false;
      }
      if (!backendTriggered && opts.manual) setStatus(`静态刷新… ${hh}:${mm}`);
    }

    if (opts.manual) {
      const st0 = await loadStatus();
      const running0 = !!(st0 && st0.update && st0.update.running);
      if (backendTriggered || backendRunning || running0) scheduleFollowRefresh(prevMetaUpdatedAt);
    }

    showProgress(18, "读取快照...");
    const params = getParams();
    const toggles = {
      condCloseMa: !!($("condCloseMa") && $("condCloseMa").checked),
      condMa: !!($("condMa") && $("condMa").checked),
      condRsi: !!($("condRsi") && $("condRsi").checked),
      condEma: !!($("condEma") && $("condEma").checked),
      condBollUp: !!($("condBollUp") && $("condBollUp").checked),
      condBollDown: !!($("condBollDown") && $("condBollDown").checked),
      condSuper: !!($("condSuper") && $("condSuper").checked),
      condKdj: !!($("condKdj") && $("condKdj").checked),
      condObv: !!($("condObv") && $("condObv").checked),
      condStochRsi: !!($("condStochRsi") && $("condStochRsi").checked),
    };
    const dynamicEnabled = toggles.condEma || toggles.condBollUp || toggles.condBollDown || toggles.condSuper || toggles.condKdj || toggles.condObv || toggles.condStochRsi;
    const needCustom = (state.customFactors || []).some((f) => f && (f.enabled || f.show));
    const useEnriched = !!opts.forceEnriched || dynamicEnabled || needCustom;

    let latest;
    if (useEnriched) {
      const { r: rLatest, j: jLatest } = await postJson("./api/latest_enriched", { custom_factors: state.customFactors, params, toggles, tail: 360 });
      if (!rLatest || !rLatest.ok || !jLatest || !jLatest.ok) {
        const msg0 = jLatest && (jLatest.message || jLatest.error) ? (jLatest.message || jLatest.error) : "读取失败";
        throw new Error(String(msg0));
      }
      latest = jLatest;
    } else {
      latest = await loadJson("./data/latest.json");
      if (!latest || !latest.results) throw new Error("读取失败");
    }
    state.latest = latest;

    showProgress(40, "构建列与排序...");
    saveParams(params);
    const customFactors = state.customFactors;
    const displayFields = buildDisplayFields(params, customFactors);
    buildSortOptionsFromFields(displayFields, state.meta && state.meta.default_sort);
    buildTableHeader(displayFields);

    showProgress(65, "计算指标与筛选...");
    const allRows = Array.isArray(latest.results) ? latest.results : [];
    const { selected, exprErrors, exprMissing, missingBuiltins, hasSeries } = applyAllFilters(allRows, params, customFactors);

    showProgress(85, "排序与渲染...");
    const { sorted, sortKey } = sortRows(selected, displayFields);
    assignRank(sorted, sortKey, displayFields);
    renderTable(sorted, displayFields);
    syncSelectedRowFrom(sorted);

    const parts = [];
    if (exprErrors) parts.push(`表达式异常：${exprErrors}`);
    if (exprMissing) parts.push(`因子缺数据：${exprMissing}`);
    const msg = parts.length ? ` ｜ ${parts.join(" ｜ ")}` : "";
    const total = params.market === "all" ? allRows.length : allRows.filter((r) => String(r.market) === String(params.market)).length;
    $("counts").textContent = `显示：${sorted.length} / ${total}${msg}`;
    updateSummary(latest.summary || {});

    showProgress(100, "完成");
    setTimeout(() => hideProgress(), 160);

    const hitText = sorted.length > 0 ? ` ｜ 命中：${sorted.length}` : " ｜ 命中：0";
    if (opts.factorCompute) setStatus(`因子计算完成${hitText}`);
    else if (isAuto) setStatus(`自动更新完成 ${hh}:${mm}${hitText}`);
    else if (opts.manual) setStatus(`${backendTriggered ? "刷新完成" : "静态刷新完成"} ${hh}:${mm}${hitText}`);
    else setStatus(`${hitText.trim()}`);
  } catch (e) {
    hideProgress();
    $("counts").textContent = `刷新失败：${String(e && e.message ? e.message : e)}`;
    if (opts.auto) setStatus("自动更新失败");
    throw e;
  }
}

function scheduleHourlyAutoUpdate() {
  let lastMetaUpdatedAt = null;
  let lastTickHourKey = null;

  const tick = async () => {
    const now = new Date();
    const hourKey = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, "0")}${String(now.getDate()).padStart(2, "0")}${String(now.getHours()).padStart(2, "0")}`;

    try {
      const meta = await loadJson("./data/meta.json");
      const updatedAt = meta && meta.updated_at ? String(meta.updated_at) : "";
      if (updatedAt && updatedAt !== lastMetaUpdatedAt) {
        lastMetaUpdatedAt = updatedAt;
        state.meta = meta;
        refresh({ auto: true, skipBackend: true });
        return;
      }
    } catch {}

    if (lastTickHourKey === hourKey) return;
    if (now.getMinutes() !== 0) return;
    lastTickHourKey = hourKey;
    refresh({ auto: true });
  };

  setInterval(tick, 60000);
  tick();
}

function initControls(meta) {
  const cfg = meta.config || {};

  const themeToggle = $("themeToggle");
  const body = document.body;
  const themeIcon = $("themeIcon");
  if (themeToggle && themeIcon) {
    const updateThemeUI = (isLight) => {
      if (isLight) {
        body.classList.add("light-theme");
        themeIcon.innerHTML = `<path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"></path>`;
      } else {
        body.classList.remove("light-theme");
        themeIcon.innerHTML = `
          <circle cx="12" cy="12" r="5"></circle>
          <line x1="12" y1="1" x2="12" y2="3"></line>
          <line x1="12" y1="21" x2="12" y2="23"></line>
          <line x1="4.22" y1="4.22" x2="5.64" y2="5.64"></line>
          <line x1="18.36" y1="18.36" x2="19.78" y2="19.78"></line>
          <line x1="1" y1="12" x2="3" y2="12"></line>
          <line x1="21" y1="12" x2="23" y2="12"></line>
          <line x1="4.22" y1="19.78" x2="5.64" y2="18.36"></line>
          <line x1="18.36" y1="5.64" x2="19.78" y2="4.22"></line>
        `;
      }
      renderKline(state.selectedRow);
    };
    const savedTheme = localStorage.getItem("screener_theme");
    updateThemeUI(savedTheme === "light");
    themeToggle.addEventListener("click", () => {
      const isLight = body.classList.toggle("light-theme");
      localStorage.setItem("screener_theme", isLight ? "light" : "dark");
      updateThemeUI(isLight);
    });
  }

  $("maPeriodClose").value = String(cfg.cond_ma_slow || 20);
  $("maFast").value = String(cfg.cond_ma_fast || 10);
  $("maSlow").value = String(cfg.cond_ma_slow || 20);
  $("rsiPeriod").value = String(cfg.cond_rsi_period || 14);
  $("rsiThreshold").value = Number(cfg.cond_rsi_threshold || 60);

  $("condCloseMa").checked = true;
  $("condMa").checked = false;
  $("condRsi").checked = true;

  // New built-ins default values
  if ($("emaPeriod")) $("emaPeriod").value = "20";
  if ($("bollPeriod")) $("bollPeriod").value = "20";
  if ($("bollStd")) $("bollStd").value = "2.0";
  if ($("bollDownPeriod")) $("bollDownPeriod").value = "20";
  if ($("bollDownStd")) $("bollDownStd").value = "2.0";
  if ($("superAtrPeriod")) $("superAtrPeriod").value = "10";
  if ($("superMult")) $("superMult").value = "3.0";
  if ($("kdjN")) $("kdjN").value = "9";
  if ($("kdjM1")) $("kdjM1").value = "3";
  if ($("kdjM2")) $("kdjM2").value = "3";
  if ($("obvMaPeriod")) $("obvMaPeriod").value = "20";
  if ($("stochRsiP")) $("stochRsiP").value = "14";
  if ($("stochRsiK")) $("stochRsiK").value = "14";
  if ($("stochRsiSmK")) $("stochRsiSmK").value = "3";
  if ($("stochRsiSmD")) $("stochRsiSmD").value = "3";

  $("btnRefresh").addEventListener("click", () => refresh({ manual: true, fetchMode: false }));
  $("sortKey").addEventListener("change", () => rerenderFromLatest());
  $("sortOrder").addEventListener("change", () => rerenderFromLatest());

  const ids = [
    "maPeriodClose", "maFast", "maSlow", "rsiPeriod", "rsiThreshold",
    "condCloseMa", "condMa", "condRsi",
    "emaPeriod", "bollPeriod", "bollStd", "bollDownPeriod", "bollDownStd", "superAtrPeriod", "superMult",
    "kdjN", "kdjM1", "kdjM2", "obvMaPeriod", "stochRsiP", "stochRsiK", "stochRsiSmK", "stochRsiSmD",
    "condEma", "condBollUp", "condBollDown", "condSuper", "condKdj", "condObv", "condStochRsi"
  ];

  for (const id of ids) {
    const el = $(id);
    if (!el) continue;
    const dyn = new Set(["condEma", "condBollUp", "condBollDown", "condSuper", "condKdj", "condObv", "condStochRsi"]);
    if (dyn.has(id)) {
      el.addEventListener("change", () => (el.checked ? refreshFactors() : rerenderFromLatest()));
      continue;
    }
    if (id.startsWith("cond")) {
      el.addEventListener("change", () => rerenderFromLatest());
      continue;
    }
    el.addEventListener("change", () => refreshFactors());
  }

  $("exprAdd").addEventListener("click", () => upsertCustomFactor());
  $("exprToggle").addEventListener("click", () => setExprEditorOpen(!state.exprEditorOpen));
  $("exprEnable").addEventListener("change", () => syncExprThresholdUI());
  $("exprHelp").addEventListener("click", () => setHelpOpen(true));
  $("helpClose").addEventListener("click", () => setHelpOpen(false));
  $("helpModal").addEventListener("click", (e) => {
    if (e.target && e.target.id === "helpModal") setHelpOpen(false);
  });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      setHelpOpen(false);
      setModalOpen("updateModal", false);
      setModalOpen("tutorialModal", false);
      setModalOpen("wecomModal", false);
      setModalOpen("feedbackModal", false);
      closeProfileMenu();
    }
  });

  $("exprText").addEventListener("input", () => {
    const t = ($("exprText").value || "").trim();
    const c = detectTemplateParamCount(t);
    renderExprParams(c);

    // Live detection for UI feedback
    try {
      const tokens = tokenizeExpr(t);
      const ast = parseExprTokens(tokens);
      if (ast.k === "cmp" && (ast.a.k === "num" || ast.b.k === "num")) {
        $("exprEnable").checked = true;
        syncExprThresholdUI();
        if (ast.b.k === "num") {
          $("exprThreshold").value = ast.b.v;
          $("exprCmp").value = ast.op;
        } else {
          $("exprThreshold").value = ast.a.v;
          const inverse = { ">": "<", ">=": "<=", "<": ">", "<=": ">=", "==": "==", "!=": "!=" };
          $("exprCmp").value = inverse[ast.op] || ast.op;
        }
      }
    } catch (e) {}
  });

  syncExprThresholdUI();
  setHelpOpen(false);

  const profileBtn = $("profileBtn");
  const profileMenu = $("profileMenu");
  if (profileBtn && profileMenu) {
    profileBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      const open = profileMenu.classList.contains("hidden");
      if (open) profileMenu.classList.remove("hidden");
      else closeProfileMenu();
    });
  }
  document.addEventListener("click", (e) => {
    const menu = $("profileMenu");
    if (!menu || menu.classList.contains("hidden")) return;
    if (e.target && (menu.contains(e.target) || ($("profileBtn") && $("profileBtn").contains(e.target)))) return;
    closeProfileMenu();
  });

  $("menuUpdateNotes").addEventListener("click", () => {
    closeProfileMenu();
    setModalOpen("updateModal", true);
    (async () => {
      try {
        const r = await fetch("./update_notes.md?t=" + Date.now(), { cache: "no-store" });
        if (!r.ok) {
          setText("updateNotes", "暂无更新说明");
          return;
        }
        const txt = await r.text();
        setText("updateNotes", (txt || "").trim() || "暂无更新说明");
      } catch {
        setText("updateNotes", "加载失败");
      }
    })();
  });
  $("updateClose").addEventListener("click", () => setModalOpen("updateModal", false));
  $("updateModal").addEventListener("click", (e) => { if (e.target && e.target.id === "updateModal") setModalOpen("updateModal", false); });

  $("menuTutorial").addEventListener("click", () => { closeProfileMenu(); setModalOpen("tutorialModal", true); });
  $("tutorialClose").addEventListener("click", () => setModalOpen("tutorialModal", false));
  $("tutorialModal").addEventListener("click", (e) => { if (e.target && e.target.id === "tutorialModal") setModalOpen("tutorialModal", false); });

  $("menuHelp").addEventListener("click", () => {
    const sub = $("helpSubmenu");
    const btn = $("menuHelp");
    if (!sub || !btn) return;
    const open = sub.classList.contains("hidden");
    sub.classList.toggle("hidden", !open);
    btn.setAttribute("aria-expanded", open ? "true" : "false");
  });

  $("menuFeedback").addEventListener("click", () => {
    closeProfileMenu();
    setText("feedbackResult", "-");
    if ($("feedbackText")) $("feedbackText").value = "";
    setModalOpen("feedbackModal", true);
  });

  $("menuBaseConfig").addEventListener("click", () => {
    closeProfileMenu();
    openBaseConfigModal();
  });
  $("feedbackClose").addEventListener("click", () => setModalOpen("feedbackModal", false));
  $("feedbackModal").addEventListener("click", (e) => { if (e.target && e.target.id === "feedbackModal") setModalOpen("feedbackModal", false); });
  $("feedbackSend").addEventListener("click", async () => {
    const msg = ($("feedbackText").value || "").trim();
    if (!msg) {
      setText("feedbackResult", "请输入反馈内容");
      return;
    }
    setText("feedbackResult", "发送中...");
    try {
      const { r, j } = await postJson("./api/feedback", { message: msg });
      if (!r.ok) {
        const missing = j && Array.isArray(j.missing) ? j.missing.filter(Boolean) : [];
        if (j && j.error === "email_not_configured") setText("feedbackResult", missing.length ? ("邮箱服务未配置，缺少：" + missing.join("、")) : "邮箱服务未配置");
        else setText("feedbackResult", (j && j.message) || "发送失败");
        return;
      }
      setText("feedbackResult", "已发送");
    } catch {
      setText("feedbackResult", "发送失败");
    }
  });

  $("wecomClose").addEventListener("click", () => setModalOpen("wecomModal", false));
  $("wecomModal").addEventListener("click", (e) => { if (e.target && e.target.id === "wecomModal") setModalOpen("wecomModal", false); });
  const baseTabs = [
    ["basecfgTabPlans", "plans"],
    ["basecfgTabMarket", "market"],
    ["basecfgTabStrategy", "strategy"],
    ["basecfgTabPush", "push"],
  ];
  for (const [id, pane] of baseTabs) {
    const el = $(id);
    if (!el) continue;
    el.addEventListener("click", () => setBaseCfgPane(pane));
  }
  if ($("baseMarketApply")) {
    $("baseMarketApply").addEventListener("click", async () => {
      const v = ($("baseMarketSelect") && $("baseMarketSelect").value) ? $("baseMarketSelect").value : "all";
      setSelectedMarket(v);
      if (state.strategyDraft && state.strategyDraft.params) state.strategyDraft.params.market = getSelectedMarket();
      updateWecomSummary(state.strategyDraft);
      setText("wecomResult", "市场已应用");
      rerenderFromLatest();
    });
  }
  if ($("strategySelect")) {
    $("strategySelect").addEventListener("change", () => {
      const g = getSelectedStrategyGroup();
      if ($("strategyName")) $("strategyName").value = g && g.name ? String(g.name) : "";
      if (g && g.config) state.strategyDraft = normalizeStrategyConfig(g.config);
      else state.strategyDraft = normalizeStrategyConfig(buildWecomConfigFromCurrent());
      if (state.strategyDraft && state.strategyDraft.params && state.strategyDraft.params.market) setSelectedMarket(state.strategyDraft.params.market);
      renderStrategyCondEditor();
      updateWecomSummary(state.strategyDraft);
      setText("wecomResult", "-");
    });
  }
  $("strategySaveCurrent").addEventListener("click", () => {
    const name = ($("strategyName").value || "").trim();
    if (!name) {
      setText("wecomResult", "请填写策略组名称");
      return;
    }
    const draft = normalizeStrategyConfig(state.strategyDraft || buildWecomConfigFromCurrent());
    if (!strategyHasAnyCondition(draft)) {
      setText("wecomResult", "请至少选择一个条件后再保存策略组");
      return;
    }
    const g0 = getSelectedStrategyGroup();
    const id = g0 && g0.id ? String(g0.id) : String(Date.now());
    const cfg0 = mergeConfigWithBase(draft);
    const cfg = {
      ...cfg0,
      customFactors: Array.isArray(cfg0.customFactors) ? cfg0.customFactors.map((f) => ({ ...f, showColumn: (f && f.show !== undefined) ? !!f.show : undefined })) : [],
    };
    const gs = Array.isArray(state.strategyGroups) ? state.strategyGroups.slice() : [];
    const idx = gs.findIndex((x) => String(x && x.id ? x.id : "") === id);
    if (idx >= 0) gs[idx] = { ...gs[idx], id, name, config: cfg };
    else gs.push({ id, name, config: cfg });
    state.strategyGroups = gs;
    saveStrategyGroups(gs);
    renderStrategyGroupSelect();
    if ($("strategySelect")) $("strategySelect").value = id;
    setText("wecomResult", idx >= 0 ? "策略组已更新" : "策略组已保存");
  });
  $("strategyApply").addEventListener("click", async () => {
    const draft = normalizeStrategyConfig(state.strategyDraft || buildWecomConfigFromCurrent());
    if (!strategyHasAnyCondition(draft)) {
      setText("wecomResult", "请至少选择一个条件后再应用");
      return;
    }
    const cfg0 = mergeConfigWithBase(draft);
    const cfg = {
      ...cfg0,
      customFactors: Array.isArray(cfg0.customFactors) ? cfg0.customFactors.map((f) => ({ ...f, showColumn: (f && f.show !== undefined) ? !!f.show : undefined })) : [],
    };
    applyConfigToUI(cfg);
    setText("wecomResult", "已应用到主页");
    await refresh({ skipBackend: true });
  });
  $("strategyDelete").addEventListener("click", () => {
    const g = getSelectedStrategyGroup();
    if (!g) {
      setText("wecomResult", "请选择策略组");
      return;
    }
    const gs = Array.isArray(state.strategyGroups) ? state.strategyGroups.slice() : [];
    const next = gs.filter((x) => String(x.id || "") !== String(g.id || ""));
    state.strategyGroups = next;
    saveStrategyGroups(next);
    renderStrategyGroupSelect();
    state.strategyDraft = normalizeStrategyConfig(buildWecomConfigFromCurrent());
    renderStrategyCondEditor();
    updateWecomSummary(state.strategyDraft);
    setText("wecomResult", "策略组已删除");
  });
  $("saveBaseConfig").addEventListener("click", async () => {
    const wl = parseSymbolListText($("whitelistText").value || "");
    const bl0 = parseSymbolListText($("blacklistText").value || "");
    const bl = bl0.length ? bl0 : defaultBlacklist.slice();
    const base0 = state.baseConfig || loadBaseConfig();
    const cfg = { ...(base0 || {}), whitelist: wl, blacklist: bl };
    state.baseConfig = cfg;
    saveBaseConfig(cfg);
    setText("wecomResult", "黑白名单已保存");
    await refresh({ skipBackend: true });
  });
  $("wecomImport").addEventListener("click", () => {
    state.strategyDraft = normalizeStrategyConfig(buildWecomConfigFromCurrent());
    renderStrategyCondEditor();
    updateWecomSummary(state.strategyDraft);
    setText("wecomResult", "已导入当前筛选");
  });

  $("wecomSave").addEventListener("click", async () => {
    const webhook_url = ($("wecomWebhook").value || "").trim();
    const enabled = !!($("wecomEnabled") && $("wecomEnabled").checked);
    const top_n = Number($("wecomTopN").value || 20);
    const config = mergeConfigWithBase(normalizeStrategyConfig(state.strategyDraft || buildWecomConfigFromCurrent()));
    try {
      const { r, j } = await postJson("./api/wecom_config", { webhook_url, enabled, top_n, config });
      if (!r.ok) {
        setText("wecomResult", (j && j.message) || "保存失败");
        return;
      }
      setText("wecomResult", "配置已保存");
      updateWecomSummary(config);
    } catch {
      setText("wecomResult", "保存失败");
    }
  });

  $("wecomSendNow").addEventListener("click", async () => {
    const webhook_url = ($("wecomWebhook").value || "").trim();
    const top_n = Number($("wecomTopN").value || 20);
    const config = mergeConfigWithBase(normalizeStrategyConfig(state.strategyDraft || buildWecomConfigFromCurrent()));
    try {
      const { r, j } = await postJson("./api/wecom/send_now", { webhook_url, top_n, config });
      if (!r.ok) {
        const d = j && j.detail ? j.detail : null;
        const parts = [];
        if (d && d.text) parts.push(`文字：${d.text.ok ? "成功" : "失败"}${d.text.message ? "（" + d.text.message + "）" : ""}`);
        if (d && d.image) parts.push(`图片：${d.image.ok ? "成功" : "失败"}${d.image.message ? "（" + d.image.message + "）" : ""}`);
        setText("wecomResult", parts.length ? parts.join(" ｜ ") : ((j && j.message) || "发送失败"));
        return;
      }
      const d = j && j.detail ? j.detail : null;
      const parts = [];
      if (d && d.text) parts.push(`文字：${d.text.ok ? "成功" : "失败"}${d.text.message ? "（" + d.text.message + "）" : ""}`);
      if (d && d.image) parts.push(`图片：${d.image.ok ? "成功" : "失败"}${d.image.message ? "（" + d.image.message + "）" : ""}`);
      if (parts.length) setText("wecomResult", parts.join(" ｜ "));
      else setText("wecomResult", (j && j.message) || "已发送");
    } catch {
      setText("wecomResult", "发送失败");
    }
  });

  $("emailSendNow").addEventListener("click", async () => {
    const to_email = ($("sendEmailTo").value || "").trim();
    if (!to_email) {
      setText("wecomResult", "请填写收件人邮箱");
      return;
    }
    try {
      localStorage.setItem("crypto_screener_send_email_to_v1", to_email);
    } catch {}
    const top_n = Number($("wecomTopN").value || 20);
    const config = mergeConfigWithBase(normalizeStrategyConfig(state.strategyDraft || buildWecomConfigFromCurrent()));
    try {
      const { r, j } = await postJson("./api/email/send_now", { to_email, top_n, config });
      if (!r.ok) {
        setText("wecomResult", (j && j.message) || "发送失败");
        return;
      }
      setText("wecomResult", "已发送");
    } catch {
      setText("wecomResult", "发送失败");
    }
  });

  $("menuLogout").addEventListener("click", async () => {
    closeProfileMenu();
    try {
      await postJson("./api/logout", {});
    } catch {}
    window.location.href = "./login.html?next=%2F";
  });

  window.addEventListener("resize", () => {
    renderKline(state.selectedRow);
  });
}

async function boot() {
  hideBootOverlay();
  showProgress(8, "加载配置...");
  const meta = await loadJson("./data/meta.json");
  state.meta = meta;

  const localFactors = loadCustomFactors();
  state.customFactors = localFactors;
  state.columnVisibility = loadColumnVisibility();
  state.baseConfig = loadBaseConfig();
  state.strategyGroups = loadStrategyGroups();
  updateFolderOptions();
  renderFolderConditions();
  renderCustomFactorList();
  setExprEditorOpen(false);
  renderExprParams(detectTemplateParamCount($("exprText") && $("exprText").value));
  syncExprThresholdUI();

  showProgress(35, "初始化界面...");
  initControls(meta);
  scheduleHourlyAutoUpdate();
  try {
    const u = await loadMe();
    const email = u && u.email ? String(u.email) : "";
    const username = u && u.username ? String(u.username) : "";
    const el = $("profileEmail");
    if (el) el.textContent = email || "-";
    const av = $("profileAvatar");
    if (av) av.textContent = formatEmailForAvatar(email, username);
    try {
      const j = await loadJson("./api/custom_factors");
      const fs = j && j.factors ? j.factors : null;
      if (Array.isArray(fs) && fs.length) {
        state.customFactors = fs;
        saveCustomFactors(state.customFactors);
        updateFolderOptions();
        renderFolderConditions();
        renderCustomFactorList();
      } else if (localFactors && localFactors.length) {
        syncCustomFactorsToServer(localFactors);
      }
    } catch {}
  } catch {}
  await refresh({ skipBackend: true });
}

const pageMode = (document.body && document.body.dataset && document.body.dataset.page) ? document.body.dataset.page : "main";
if (pageMode === "main") {
  boot().catch((e) => {
    hideBootOverlay();
    hideProgress();
    const el = $("summary");
    if (el) el.textContent = String(e && e.message ? e.message : e);
  });
}
