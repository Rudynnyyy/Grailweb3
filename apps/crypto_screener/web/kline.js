(function () {
const plotKeysStorageKey = "crypto_screener_plot_keys_v1";
const paramsStorageKey = "crypto_screener_params_v1";
const selectedKeyStorageKey = "crypto_screener_selected_key_v1";
const themeStorageKey = "screener_theme";
const placementStorageKey = "crypto_screener_kline_placement_v1";
const tailStorageKey = "crypto_screener_kline_tail_v2";
const hiddenKeysStorageKey = "crypto_screener_kline_hidden_keys_v1";
const customFactorsStorageKey = "crypto_screener_custom_factors_v1";
const lastPicksStorageKey = "crypto_screener_last_picks_v1";
const axisL = 72;
try {
  window.__klineStarted = true;
} catch {}
const viewState = {
  key: "",
  row: null,
  latestSummary: null,
  latestRows: [],
  barHours: 1,
  dt: [],
  timesMs: [],
  params: {},
  plotKeys: [],
  overlaysAll: [],
  indicatorsAll: [],
  indicatorPanels: [],
  hiddenKeys: {},
  customFactorsById: {},
  lockY: false,
  viewN: 360,
  offset: 0,
  start: 0,
  n: 0,
  end: 0,
  n0: 0,
  raf: 0,
  dragging: false,
  dragStartX: 0,
  dragStartOffset: 0,
  hoverLocalIdx: null,
  pickQuery: "",
  pickRows: [],
  pickLabel: "",
  pickRankMap: null,
  selActive: false,
  selStartLocal: null,
  selEndLocal: null,
  selResult: null,
};

function loadPlacement() {
  try {
    const raw = localStorage.getItem(placementStorageKey);
    const obj = raw ? JSON.parse(raw) : {};
    return obj && typeof obj === "object" ? obj : {};
  } catch {
    return {};
  }
}

function savePlacement(obj) {
  try {
    localStorage.setItem(placementStorageKey, JSON.stringify(obj || {}));
  } catch {}
}

function loadTailSetting(defaultValue) {
  let raw = null;
  try {
    raw = localStorage.getItem(tailStorageKey);
  } catch {}
  const base = raw === null ? Number(defaultValue || 720) : Number(raw);
  if (base === 360 || base === 720 || base === 1440 || base === 2160 || base === 3650) return base;
  return 720;
}

function saveTailSetting(v) {
  try {
    localStorage.setItem(tailStorageKey, String(v));
  } catch {}
}

function loadHiddenKeys() {
  try {
    const raw = localStorage.getItem(hiddenKeysStorageKey);
    const obj = raw ? JSON.parse(raw) : {};
    return obj && typeof obj === "object" ? obj : {};
  } catch {
    return {};
  }
}

function saveHiddenKeys(map) {
  try {
    localStorage.setItem(hiddenKeysStorageKey, JSON.stringify(map || {}));
  } catch {}
}

function isKeyHidden(key) {
  const m = viewState.hiddenKeys || {};
  return !!m[String(key || "")];
}

function setKeyHidden(key, hidden) {
  const k = String(key || "");
  if (!k) return;
  const m = { ...(viewState.hiddenKeys || {}) };
  if (hidden) m[k] = true;
  else delete m[k];
  viewState.hiddenKeys = m;
  saveHiddenKeys(m);
}

function updateChipBar(host, items) {
  if (!host) return;
  host.innerHTML = "";
  const arr = Array.isArray(items) ? items : [];
  for (const it of arr) {
    const k = String(it && it.key ? it.key : "");
    if (!k) continue;
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "kline-chip";
    const dot = document.createElement("span");
    dot.className = "kline-chip-dot";
    dot.style.background = String(it.color || "#94a3b8");
    const label = document.createElement("span");
    label.textContent = String(it.name || k);
    const off = isKeyHidden(k);
    if (off) btn.classList.add("off");
    btn.appendChild(dot);
    btn.appendChild(label);
    btn.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      const nextOff = !isKeyHidden(k);
      setKeyHidden(k, nextOff);
      btn.classList.toggle("off", nextOff);
      scheduleDraw();
    });
    host.appendChild(btn);
  }
}

function pickKey(row) {
  return `${String(row && row.market ? row.market : "")}|${String(row && row.symbol ? row.symbol : "")}`;
}

function setSelectedKey(nextKey) {
  const k = String(nextKey || "");
  if (!k) return;
  try {
    localStorage.setItem(selectedKeyStorageKey, k);
  } catch {}
  try {
    const u = new URL(window.location.href);
    u.searchParams.set("key", k);
    window.history.replaceState({}, "", u.toString());
  } catch {}
}

function renderPickList(rows, activeKey) {
  const host = document.getElementById("pickList");
  if (!host) return;
  const q = String(viewState.pickQuery || "").trim().toUpperCase();
  const arr0 = Array.isArray(rows) ? rows : [];
  const rankMap = viewState.pickRankMap instanceof Map ? viewState.pickRankMap : new Map();
  const arr = arr0
    .filter((r) => {
      if (!q) return true;
      const s = stripQuote(r && r.symbol ? r.symbol : "").toUpperCase();
      return s.includes(q);
    })
    .slice()
    .sort((a, b) => {
      const ka = pickKey(a);
      const kb = pickKey(b);
      const ra = Number(rankMap.get(ka));
      const rb = Number(rankMap.get(kb));
      const va = Number.isFinite(ra) ? ra : 1e12;
      const vb = Number.isFinite(rb) ? rb : 1e12;
      if (va !== vb) return va - vb;
      const sa = stripQuote(a && a.symbol ? a.symbol : "").toUpperCase();
      const sb = stripQuote(b && b.symbol ? b.symbol : "").toUpperCase();
      if (sa !== sb) return sa < sb ? -1 : 1;
      return ka < kb ? -1 : (ka > kb ? 1 : 0);
    });

  host.innerHTML = "";
  for (const r of arr) {
    const key = pickKey(r);
    const item = document.createElement("div");
    item.className = "kline-pick-item";
    item.classList.toggle("active", key === String(activeKey || ""));
    const rankEl = document.createElement("div");
    rankEl.className = "kline-pick-rank";
    const rr = Number(rankMap.get(key));
    rankEl.textContent = Number.isFinite(rr) ? `#${Math.trunc(rr)}` : "";
    const nameEl = document.createElement("div");
    nameEl.className = "kline-pick-name";
    nameEl.textContent = stripQuote(r && r.symbol ? r.symbol : "");
    const sub = document.createElement("div");
    sub.className = "kline-pick-sub";
    const market = document.createElement("span");
    market.textContent = marketLabel(r && r.market ? r.market : "");
    const close = document.createElement("span");
    close.textContent = `收盘 ${fmtNum(r && r.close)}`;
    const pct = document.createElement("span");
    const p0 = Number(r && r.pct_change);
    if (Number.isFinite(p0)) {
      pct.textContent = `${p0.toFixed(2)}%`;
      pct.className = p0 >= 0 ? "up" : "down";
    } else {
      pct.textContent = "-";
    }
    sub.appendChild(market);
    sub.appendChild(close);
    sub.appendChild(pct);
    item.appendChild(rankEl);
    item.appendChild(nameEl);
    item.appendChild(sub);
    item.addEventListener("click", () => {
      if (!key || key === String(viewState.key || "")) return;
      setSelectedKey(key);
      render();
    });
    host.appendChild(item);
  }

  const meta = document.getElementById("pickMeta");
  if (meta) {
    const total = arr0.length;
    const shown = arr.length;
    const head = String(viewState.pickLabel || "");
    const base = q ? `匹配 ${shown}/${total}` : `共 ${total}`;
    meta.textContent = head ? `${head}｜${base}` : base;
  }
}

function loadLastPicks() {
  try {
    const raw = localStorage.getItem(lastPicksStorageKey);
    const j = raw ? JSON.parse(raw) : null;
    if (!j || typeof j !== "object") return null;
    const rows = Array.isArray(j.rows) ? j.rows : [];
    const summary = j.summary && typeof j.summary === "object" ? j.summary : {};
    const saved_at = String(j.saved_at || "");
    return { rows, summary, saved_at };
  } catch {
    return null;
  }
}

function loadCustomFactorsById() {
  try {
    const raw = localStorage.getItem(customFactorsStorageKey);
    const arr = raw ? JSON.parse(raw) : [];
    const out = {};
    if (!Array.isArray(arr)) return out;
    for (const it of arr) {
      if (!it || !it.id) continue;
      out[String(it.id)] = it;
    }
    return out;
  } catch {
    return {};
  }
}

function lastFinite(arr) {
  const a = Array.isArray(arr) ? arr : [];
  for (let i = a.length - 1; i >= 0; i--) {
    const v = Number(a[i]);
    if (Number.isFinite(v)) return v;
  }
  return null;
}

function showAlert(text) {
  const el = document.getElementById("klineAlert");
  if (!el) return;
  if (!text) {
    el.classList.add("hidden");
    el.textContent = "";
    return;
  }
  el.classList.remove("hidden");
  el.textContent = String(text);
}

function wireGlobalErrorHandler() {
  const ignore = (msg) => {
    const s = String(msg || "");
    if (!s) return false;
    if (s.includes("chrome-extension://")) return true;
    if (s.includes("SubtleCrypto not available")) return true;
    return false;
  };
  window.addEventListener("error", (e) => {
    const msg = e && e.error && e.error.stack ? e.error.stack : (e && e.message ? e.message : String(e));
    if (ignore(msg)) return;
    showAlert(`脚本错误：\n${msg}`);
  });
  window.addEventListener("unhandledrejection", (e) => {
    const reason = e && e.reason ? e.reason : e;
    const msg = reason && reason.stack ? reason.stack : String(reason);
    if (ignore(msg)) return;
    showAlert(`Promise 错误：\n${msg}`);
  });
}

function loadJsonNoCache(url) {
  return loadJson(url);
}

function parseQuery() {
  const u = new URL(window.location.href);
  return {
    key: u.searchParams.get("key") || "",
  };
}

function updateBinanceLink(row) {
  const el = document.getElementById("binanceLink");
  if (!el) return;
  if (!row || !row.symbol || !row.market) {
    el.classList.add("hidden");
    el.setAttribute("href", "#");
    return;
  }
  const sym = String(row.symbol || "").toUpperCase();
  const market = String(row.market || "").toLowerCase();
  let href = "https://www.binance.com/";
  if (market === "swap") {
    href = `https://www.binance.com/en/futures/${encodeURIComponent(sym.replace(/-/g, ""))}`;
  } else {
    href = `https://www.binance.com/en/trade/${encodeURIComponent(sym.replace(/-/g, "_"))}?type=spot`;
  }
  el.classList.remove("hidden");
  el.setAttribute("href", href);
}

function loadParams() {
  try {
    const raw = localStorage.getItem(paramsStorageKey);
    return raw ? JSON.parse(raw) : {};
  } catch {
    return {};
  }
}

function loadPlotKeys() {
  try {
    const raw = localStorage.getItem(plotKeysStorageKey);
    const arr = raw ? JSON.parse(raw) : [];
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

function ensureCanvas(canvas) {
  if (!canvas) return null;
  const rect = canvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  const pw = canvas.parentElement ? canvas.parentElement.clientWidth : 0;
  const ph = canvas.parentElement ? canvas.parentElement.clientHeight : 0;
  const rw = rect.width > 0 ? rect.width : pw;
  const rh = rect.height > 0 ? rect.height : ph;
  const w = Math.max(50, Math.round(rw * dpr));
  const h = Math.max(50, Math.round(rh * dpr));
  if (canvas.width !== w) canvas.width = w;
  if (canvas.height !== h) canvas.height = h;
  const ctx = canvas.getContext("2d");
  ctx.setTransform(1, 0, 0, 1, 0, 0);
  return ctx;
}

function applyThemeFromStorage() {
  const raw = localStorage.getItem(themeStorageKey) || "dark";
  if (raw === "light") document.body.classList.add("light-theme");
  else document.body.classList.remove("light-theme");
}

function drawEmpty(ctx, text) {
  const cs = getComputedStyle(document.body);
  const bg = cs.getPropertyValue("--bg-app").trim() || "#0f172a";
  const fg = cs.getPropertyValue("--text-dim").trim() || "#94a3b8";
  const w = ctx.canvas.width;
  const h = ctx.canvas.height;
  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = bg;
  ctx.fillRect(0, 0, w, h);
  ctx.fillStyle = fg;
  ctx.font = `${Math.max(12, Math.round(14 * (window.devicePixelRatio || 1)))}px sans-serif`;
  ctx.fillText(text || "正在加载数据，请耐心等待", 14, 28);
}

function parseIsoToMs(s) {
  try {
    const ms = Date.parse(String(s || ""));
    return Number.isFinite(ms) ? ms : null;
  } catch {
    return null;
  }
}

function fmtTs(ms) {
  if (!Number.isFinite(ms)) return "";
  const d = new Date(ms);
  const MM = String(d.getMonth() + 1).padStart(2, "0");
  const DD = String(d.getDate()).padStart(2, "0");
  const hh = String(d.getHours()).padStart(2, "0");
  return `${MM}-${DD} ${hh}:00`;
}

function fmtAxisNum(v0) {
  const v = Number(v0);
  if (!Number.isFinite(v)) return "-";
  const a = Math.abs(v);
  if (a === 0) return "0";
  let dp = 2;
  if (a >= 1000) dp = 0;
  else if (a >= 1) dp = 2;
  else {
    dp = -Math.floor(Math.log10(a)) + 1;
    dp = Math.min(8, Math.max(2, dp));
  }
  let s = v.toFixed(dp);
  if (s.indexOf(".") >= 0) s = s.replace(/\.?0+$/, "");
  return s;
}

function barTimeMs(globalIdx) {
  const ts = viewState.timesMs || [];
  if (ts && globalIdx >= 0 && globalIdx < ts.length) return ts[globalIdx];
  const row = viewState.row;
  const n0 = viewState.n0;
  if (!row || !n0) return null;
  const endMs = parseIsoToMs(row.dt_close) ?? parseIsoToMs(row.dt_display) ?? parseIsoToMs((viewState.latestSummary || {}).latest_dt_close) ?? parseIsoToMs((viewState.latestSummary || {}).latest_dt_display);
  if (!endMs) return null;
  const bh = Number(viewState.barHours || 1);
  const step = bh * 3600 * 1000;
  const startMs = endMs - (n0 - 1) * step;
  return startMs + globalIdx * step;
}

function scheduleDraw() {
  if (viewState.raf) return;
  viewState.raf = requestAnimationFrame(() => {
    viewState.raf = 0;
    drawFromState();
    // 同步绘图层尺寸并重绘
    const ref = document.getElementById('candleCanvas');
    const dc = drawState.drawCanvas;
    if (ref && dc) {
      if (dc.width !== ref.width || dc.height !== ref.height) {
        dc.width = ref.width; dc.height = ref.height;
        dc.style.width = (ref.offsetWidth || ref.clientWidth) + 'px';
        dc.style.height = (ref.offsetHeight || ref.clientHeight) + 'px';
        drawState.drawCtx = dc.getContext('2d');
      }
      redrawDrawings();
    }
    if (drawState.infoVisible) updateInfoPanel();
    updateStatsPanel();
  });
}

function smaSeries(arr, window) {
  const w = Math.trunc(Number(window));
  if (!Number.isFinite(w) || w <= 0) return [];
  const out = new Array(arr.length).fill(null);
  let sum = 0;
  let cnt = 0;
  const q = [];
  for (let i = 0; i < arr.length; i++) {
    const v0 = arr[i];
    if (!Number.isFinite(Number(v0))) {
      q.push(null);
      if (q.length > w) q.shift();
      out[i] = null;
      continue;
    }
    const v = Number(v0);
    q.push(v);
    sum += v;
    cnt += 1;
    if (q.length > w) {
      const x = q.shift();
      if (Number.isFinite(Number(x))) {
        sum -= Number(x);
        cnt -= 1;
      }
    }
    if (q.length === w && cnt === w) out[i] = sum / w;
    else out[i] = null;
  }
  return out;
}

function clampInt(x, lo, hi) {
  const v = Math.trunc(Number(x));
  if (!Number.isFinite(v)) return lo;
  if (v < lo) return lo;
  if (v > hi) return hi;
  return v;
}

function computeWindow() {
  const n0 = viewState.n0;
  if (!Number.isFinite(n0) || n0 <= 0) {
    viewState.start = 0;
    viewState.end = 0;
    viewState.n = 0;
    return;
  }
  const minN = 30;
  viewState.viewN = clampInt(viewState.viewN, minN, n0);
  const maxOffset = Math.max(0, n0 - viewState.viewN);
  viewState.offset = clampInt(viewState.offset, 0, maxOffset);
  const end = n0 - 1 - viewState.offset;
  const start = Math.max(0, end - viewState.viewN + 1);
  const n = end - start + 1;
  viewState.start = start;
  viewState.end = end;
  viewState.n = n;
}

function rollingStdSeries(arr, window) {
  const w = Math.trunc(Number(window));
  if (!Number.isFinite(w) || w <= 0) return [];
  const out = new Array(arr.length).fill(null);
  for (let i = 0; i < arr.length; i++) {
    if (i + 1 < w) continue;
    const tail = arr.slice(i + 1 - w, i + 1);
    if (tail.some((x) => !Number.isFinite(Number(x)))) continue;
    const xs = tail.map(Number);
    const mean = xs.reduce((a, b) => a + b, 0) / w;
    const var0 = xs.reduce((a, b) => a + (b - mean) * (b - mean), 0) / w;
    out[i] = Math.sqrt(var0);
  }
  return out;
}

function rsiSeries(closes, period) {
  const p = Math.trunc(Number(period));
  const out = new Array(closes.length).fill(null);
  if (!Number.isFinite(p) || p <= 0 || closes.length < p + 1) return out;
  const xs = closes.map((v) => (Number.isFinite(Number(v)) ? Number(v) : null));
  let avgGain = 0;
  let avgLoss = 0;
  let init = true;
  for (let i = 1; i < xs.length; i++) {
    const a = xs[i - 1];
    const b = xs[i];
    if (a === null || b === null) {
      out[i] = null;
      continue;
    }
    const d = b - a;
    const g = d > 0 ? d : 0;
    const l = d < 0 ? -d : 0;
    if (i <= p) {
      avgGain += g;
      avgLoss += l;
      if (i === p) {
        avgGain /= p;
        avgLoss /= p;
        init = false;
      }
      out[i] = null;
      continue;
    }
    if (init) {
      out[i] = null;
      continue;
    }
    avgGain = (avgGain * (p - 1) + g) / p;
    avgLoss = (avgLoss * (p - 1) + l) / p;
    if (avgLoss === 0 && avgGain === 0) out[i] = 0;
    else if (avgLoss === 0) out[i] = 100;
    else {
      const rs = avgGain / avgLoss;
      out[i] = 100 - 100 / (1 + rs);
    }
  }
  return out;
}

function supertrendSeries(highs, lows, closes, period, mult) {
  const p = Math.trunc(Number(period));
  const m = Number(mult);
  const n = Math.min(highs.length, lows.length, closes.length);
  const out = new Array(n).fill(null);
  if (!Number.isFinite(p) || p <= 0 || !Number.isFinite(m) || n < p + 1) return out;

  const trs = new Array(n).fill(null);
  for (let i = 1; i < n; i++) {
    const h = Number(highs[i]);
    const l = Number(lows[i]);
    const pc = Number(closes[i - 1]);
    if (!Number.isFinite(h) || !Number.isFinite(l) || !Number.isFinite(pc)) continue;
    trs[i] = Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc));
  }

  const atrs = new Array(n).fill(null);
  const alpha = 1 / p;
  let atr0 = null;
  for (let i = 1; i < n; i++) {
    const tr = trs[i];
    if (tr === null) continue;
    if (atr0 === null) {
      atr0 = tr;
    } else {
      atr0 = alpha * tr + (1 - alpha) * atr0;
    }
    atrs[i] = atr0;
  }

  let trend = 1;
  let upperBand = null;
  let lowerBand = null;

  for (let i = 0; i < n; i++) {
    const h = Number(highs[i]);
    const l = Number(lows[i]);
    const c = Number(closes[i]);
    const a = atrs[i];
    if (!Number.isFinite(h) || !Number.isFinite(l) || !Number.isFinite(c) || a === null) continue;
    const mid = (h + l) / 2;
    const basicUpper = mid + m * a;
    const basicLower = mid - m * a;

    if (upperBand === null || lowerBand === null) {
      upperBand = basicUpper;
      lowerBand = basicLower;
    } else {
      const prevClose = Number(closes[i - 1]);
      if (Number.isFinite(prevClose)) {
        upperBand = basicUpper < upperBand || prevClose > upperBand ? basicUpper : upperBand;
        lowerBand = basicLower > lowerBand || prevClose < lowerBand ? basicLower : lowerBand;
      } else {
        upperBand = basicUpper;
        lowerBand = basicLower;
      }
    }

    if (trend === 1) {
      if (c < lowerBand) {
        trend = -1;
        out[i] = upperBand;
      } else {
        out[i] = lowerBand;
      }
    } else {
      if (c > upperBand) {
        trend = 1;
        out[i] = lowerBand;
      } else {
        out[i] = upperBand;
      }
    }
  }

  return out;
}

function kdjSeries(highs, lows, closes, n, m1, m2) {
  const N = Math.trunc(Number(n));
  const M1 = Math.trunc(Number(m1));
  const M2 = Math.trunc(Number(m2));
  const len = Math.min(highs.length, lows.length, closes.length);
  const K = new Array(len).fill(null);
  const D = new Array(len).fill(null);
  const J = new Array(len).fill(null);
  if (!Number.isFinite(N) || N <= 0 || !Number.isFinite(M1) || M1 <= 0 || !Number.isFinite(M2) || M2 <= 0) return { K, D, J };
  let k0 = 50;
  let d0 = 50;
  const a1 = 1 / M1;
  const a2 = 1 / M2;
  for (let i = 0; i < len; i++) {
    if (i + 1 < N) continue;
    const winH = highs.slice(i + 1 - N, i + 1).map(Number);
    const winL = lows.slice(i + 1 - N, i + 1).map(Number);
    const c = Number(closes[i]);
    if (winH.some((x) => !Number.isFinite(x)) || winL.some((x) => !Number.isFinite(x)) || !Number.isFinite(c)) continue;
    const hh = Math.max(...winH);
    const ll = Math.min(...winL);
    const rsv = hh === ll ? 50 : ((c - ll) / (hh - ll)) * 100;
    k0 = a1 * rsv + (1 - a1) * k0;
    d0 = a2 * k0 + (1 - a2) * d0;
    K[i] = k0;
    D[i] = d0;
    J[i] = 3 * k0 - 2 * d0;
  }
  return { K, D, J };
}

function obvSeries(closes, volumes) {
  const len = Math.min(closes.length, volumes.length);
  const out = new Array(len).fill(null);
  let v0 = 0;
  out[0] = 0;
  for (let i = 1; i < len; i++) {
    const c = Number(closes[i]);
    const pc = Number(closes[i - 1]);
    const vol = Number(volumes[i]);
    if (!Number.isFinite(c) || !Number.isFinite(pc) || !Number.isFinite(vol)) {
      out[i] = out[i - 1];
      continue;
    }
    if (c > pc) v0 += vol;
    else if (c < pc) v0 -= vol;
    out[i] = v0;
  }
  return out;
}

function stochRsiSeries(closes, rsiP, stochP, smoothK, smoothD) {
  const len = closes.length;
  const K = new Array(len).fill(null);
  const D = new Array(len).fill(null);
  const rsiS = rsiSeries(closes, rsiP);
  const sp = Math.trunc(Number(stochP));
  if (!Number.isFinite(sp) || sp <= 1) return { K, D };
  const raw = new Array(len).fill(null);
  for (let i = 0; i < len; i++) {
    if (i + 1 < sp) continue;
    const tail = rsiS.slice(i + 1 - sp, i + 1);
    if (tail.some((x) => x === null || !Number.isFinite(Number(x)))) continue;
    const low = Math.min(...tail.map(Number));
    const high = Math.max(...tail.map(Number));
    const cur = Number(rsiS[i]);
    raw[i] = high === low ? 0 : ((cur - low) / (high - low)) * 100;
  }
  const k0 = emaSeries(raw.map((x) => (x === null ? 0 : x)), smoothK);
  const d0 = emaSeries(k0.map((x) => (x === null ? 0 : x)), smoothD);
  for (let i = 0; i < len; i++) {
    if (raw[i] === null) continue;
    K[i] = k0[i];
    D[i] = d0[i];
  }
  return { K, D };
}

function seriesMinMax(arr) {
  let mn = Infinity;
  let mx = -Infinity;
  for (const v0 of arr) {
    const v = Number(v0);
    if (!Number.isFinite(v)) continue;
    if (v < mn) mn = v;
    if (v > mx) mx = v;
  }
  if (!Number.isFinite(mn) || !Number.isFinite(mx)) return null;
  if (mn === mx) return { mn: mn - 1, mx: mx + 1 };
  return { mn, mx };
}

function seriesMinMaxRange(arr, st, end) {
  const a = Array.isArray(arr) ? arr : [];
  let mn = Infinity;
  let mx = -Infinity;
  const s = Math.max(0, Math.trunc(Number(st)));
  const e = Math.min(a.length - 1, Math.trunc(Number(end)));
  for (let i = s; i <= e; i++) {
    const v = Number(a[i]);
    if (!Number.isFinite(v)) continue;
    if (v < mn) mn = v;
    if (v > mx) mx = v;
  }
  if (!Number.isFinite(mn) || !Number.isFinite(mx)) return null;
  if (mn === mx) return { mn: mn - 1, mx: mx + 1 };
  return { mn, mx };
}

function drawYAxis(ctx, L, T, ph, yMin, yMax, textColor) {
  ctx.fillStyle = textColor;
  ctx.font = `${Math.max(11, Math.round(11 * (window.devicePixelRatio || 1)))}px sans-serif`;
  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  const ticks = 5;
  const x = L - 6;
  for (let k = 0; k <= ticks; k++) {
    const yy = T + (ph * k) / ticks;
    const v = yMax - ((yMax - yMin) * k) / ticks;
    ctx.fillText(fmtAxisNum(v), x, yy);
  }
  ctx.textAlign = "start";
  ctx.textBaseline = "alphabetic";
}

function drawLineSeries(ctx, arr, mapX, mapY, color) {
  ctx.strokeStyle = color;
  ctx.lineWidth = 2;
  ctx.beginPath();
  let started = false;
  for (let i = 0; i < arr.length; i++) {
    const v0 = arr[i];
    if (v0 === null || v0 === undefined) continue;
    const v = Number(v0);
    if (!Number.isFinite(v)) continue;
    const x = mapX(i);
    const y = mapY(v);
    if (!started) {
      ctx.moveTo(x, y);
      started = true;
    } else {
      ctx.lineTo(x, y);
    }
  }
  ctx.stroke();
}

function drawCandles(ctx, row, overlays, start, n, endIdx, labelAt, hoverLocalIdx, showVolumeBar) {
  const cs = getComputedStyle(document.body);
  const border = cs.getPropertyValue("--border").trim() || "rgba(255,255,255,0.1)";
  const up = cs.getPropertyValue("--success").trim() || "#10b981";
  const dn = cs.getPropertyValue("--danger").trim() || "#ef4444";
  const text = cs.getPropertyValue("--text-dim").trim() || "#94a3b8";
  const bg = cs.getPropertyValue("--bg-app").trim() || "#0f172a";
  const accent = cs.getPropertyValue("--accent").trim() || "#3b82f6";

  const w = ctx.canvas.width;
  const h = ctx.canvas.height;
  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = bg;
  ctx.fillRect(0, 0, w, h);

  const s = row.series || {};
  const opens = s.open || [];
  const highs = s.high || [];
  const lows = s.low || [];
  const closes = s.close || [];
  const n0 = Math.min(opens.length, highs.length, lows.length, closes.length);
  if (!Number.isFinite(n0) || n0 <= 0 || !Number.isFinite(n) || n <= 0) return;
  const end = Math.min(Math.max(0, endIdx), n0 - 1);
  const st = Math.min(Math.max(0, start), end);

  let minP = Infinity;
  let maxP = -Infinity;
  for (let i = st; i <= end; i++) {
    const hi = Number(highs[i]);
    const lo = Number(lows[i]);
    if (Number.isFinite(hi)) maxP = Math.max(maxP, hi);
    if (Number.isFinite(lo)) minP = Math.min(minP, lo);
  }
  for (const ov of overlays) {
    const mm = seriesMinMax(ov.series);
    if (!mm) continue;
    minP = Math.min(minP, mm.mn);
    maxP = Math.max(maxP, mm.mx);
  }
  if (!Number.isFinite(minP) || !Number.isFinite(maxP)) return;
  const pad = (maxP - minP) * 0.06;
  const yMax = maxP + pad;
  const yMin = minP - pad;

  const L = axisL;
  const R = 10;
  const T = 10;
  const B = 22;
  // 成交量区域：只在 showVolumeBar 时留出底部22%
  const volRatio = 0.22;
  const totalH = h - T - B;
  const volH = showVolumeBar ? Math.round(totalH * volRatio) : 0;
  const candleH = totalH - volH - (showVolumeBar ? 4 : 0);
  const ph = candleH;
  const pw = w - L - R;
  const stepX = pw / Math.max(1, n);
  const bodyW = Math.max(1, Math.min(14, Math.floor(stepX * 0.65)));

  const yOf = (p) => T + ((yMax - p) / (yMax - yMin)) * ph;
  const xOf = (i) => L + i * stepX + stepX / 2;

  ctx.strokeStyle = border;
  ctx.lineWidth = 1;
  ctx.beginPath();
  for (let k = 1; k <= 4; k++) {
    const yy = T + (ph * k) / 5;
    ctx.moveTo(L, yy);
    ctx.lineTo(L + pw, yy);
  }
  ctx.stroke();
  drawYAxis(ctx, L, T, ph, yMin, yMax, text);

  for (let i = 0; i < n; i++) {
    const idx = st + i;
    const o = Number(opens[idx]);
    const hi = Number(highs[idx]);
    const lo = Number(lows[idx]);
    const c = Number(closes[idx]);
    if (!Number.isFinite(o) || !Number.isFinite(hi) || !Number.isFinite(lo) || !Number.isFinite(c)) continue;
    const color = c >= o ? up : dn;
    const x = xOf(i);
    const yO = yOf(o);
    const yC = yOf(c);
    const yH = yOf(hi);
    const yL = yOf(lo);

    ctx.strokeStyle = color;
    ctx.beginPath();
    ctx.moveTo(x, yH);
    ctx.lineTo(x, yL);
    ctx.stroke();

    ctx.fillStyle = color;
    const top = Math.min(yO, yC);
    const bot = Math.max(yO, yC);
    const bh = Math.max(1, bot - top);
    ctx.fillRect(Math.round(x - bodyW / 2), Math.round(top), bodyW, Math.round(bh));
  }

  const lastClose = Number(closes[end]);
  if (Number.isFinite(lastClose)) {
    ctx.strokeStyle = accent;
    ctx.setLineDash([4, 4]);
    ctx.beginPath();
    ctx.moveTo(L, yOf(lastClose));
    ctx.lineTo(L + pw, yOf(lastClose));
    ctx.stroke();
    ctx.setLineDash([]);
  }

  for (const ov of overlays) {
    drawLineSeries(
      ctx,
      ov.series,
      (i) => xOf(i),
      (v) => yOf(v),
      ov.color
    );
  }

  // ---- 成交量柱状图 ----
  {
    const vols = (row.series || {}).volume || [];
    const volTop = T + candleH + 4;
    const volBase = volTop + volH;
    let maxVol = 0;
    for (let i = st; i <= end; i++) {
      const v = Number(vols[i]);
      if (Number.isFinite(v) && v > maxVol) maxVol = v;
    }
    if (showVolumeBar && maxVol > 0) {
      // 成交量区域背景分隔线
      ctx.strokeStyle = border;
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(L, volTop); ctx.lineTo(L + pw, volTop);
      ctx.stroke();
      // 成交量 Y 轴标签（最大值）
      ctx.fillStyle = text;
      ctx.font = `${Math.max(9, Math.round(9 * (window.devicePixelRatio || 1)))}px sans-serif`;
      ctx.textAlign = 'right'; ctx.textBaseline = 'top';
      ctx.fillText(fmtAxisNum(maxVol), L - 4, volTop);
      ctx.textAlign = 'start'; ctx.textBaseline = 'alphabetic';
      // 绘制柱子
      for (let i = 0; i < n; i++) {
        const idx = st + i;
        const v = Number(vols[idx]);
        if (!Number.isFinite(v) || v <= 0) continue;
        const o = Number(opens[idx]), c0 = Number(closes[idx]);
        const color = (Number.isFinite(o) && Number.isFinite(c0) && c0 >= o) ? up : dn;
        const barH = Math.max(1, Math.round((v / maxVol) * volH));
        const x = xOf(i);
        ctx.fillStyle = color;
        ctx.globalAlpha = 0.55;
        ctx.fillRect(Math.round(x - bodyW / 2), volBase - barH, bodyW, barH);
      }
      ctx.globalAlpha = 1.0;
    }
  }

  if (typeof labelAt === "function" && n >= 2) {
    const idxs = [Math.round((n - 1) / 4), Math.round(((n - 1) * 2) / 4), Math.round(((n - 1) * 3) / 4)];
    const seen = new Set();
    ctx.fillStyle = text;
    ctx.font = `${Math.max(11, Math.round(11 * (window.devicePixelRatio || 1)))}px sans-serif`;
    for (const i0 of idxs) {
      const i = Math.max(0, Math.min(n - 1, i0));
      if (seen.has(i)) continue;
      seen.add(i);
      const s0 = String(labelAt(i) || "");
      if (!s0) continue;
      const x = xOf(i);
      const y = T + ph + 18;
      const m = ctx.measureText(s0);
      const tw = Math.ceil(m.width);
      const left = L + 2;
      const right = L + pw - tw - 2;
      const tx = Math.round(Math.max(left, Math.min(right, x - tw / 2)));
      ctx.fillText(s0, tx, y);
    }
  }

  if (Number.isFinite(Number(hoverLocalIdx))) {
    const hi = Math.trunc(Number(hoverLocalIdx));
    if (hi >= 0 && hi < n) {
      const x = xOf(hi);
      ctx.strokeStyle = border;
      ctx.setLineDash([3, 3]);
      ctx.beginPath();
      ctx.moveTo(x, T);
      ctx.lineTo(x, T + ph);
      ctx.stroke();
      ctx.setLineDash([]);
    }
  }
}

function drawIndicators(ctx, groups, n, labelAt, title) {
  const cs = getComputedStyle(document.body);
  const border = cs.getPropertyValue("--border").trim() || "rgba(255,255,255,0.1)";
  const text = cs.getPropertyValue("--text-dim").trim() || "#94a3b8";
  const bg = cs.getPropertyValue("--bg-app").trim() || "#0f172a";

  const w = ctx.canvas.width;
  const h = ctx.canvas.height;
  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = bg;
  ctx.fillRect(0, 0, w, h);

  const L = axisL;
  const R = 10;
  const T = 10;
  const B = 22;
  const pw = w - L - R;
  const ph = h - T - B;

  const stepX = pw / Math.max(1, n);
  const xOf = (i) => L + i * stepX + stepX / 2;

  let mn = Infinity;
  let mx = -Infinity;
  for (const g of groups) {
    const mm = seriesMinMax(g.series);
    if (!mm) continue;
    mn = Math.min(mn, mm.mn);
    mx = Math.max(mx, mm.mx);
  }
  if (!Number.isFinite(mn) || !Number.isFinite(mx)) return;
  const pad = (mx - mn) * 0.08;
  const yMax = mx + pad;
  const yMin = mn - pad;
  const yOf = (v) => T + ((yMax - v) / (yMax - yMin)) * ph;

  ctx.strokeStyle = border;
  ctx.lineWidth = 1;
  ctx.beginPath();
  for (let k = 1; k <= 3; k++) {
    const yy = T + (ph * k) / 4;
    ctx.moveTo(L, yy);
    ctx.lineTo(L + pw, yy);
  }
  ctx.stroke();
  drawYAxis(ctx, L, T, ph, yMin, yMax, text);

  for (const g of groups) {
    drawLineSeries(ctx, g.series, (i) => xOf(i), (v) => yOf(v), g.color);
  }

  if (title) {
    ctx.fillStyle = text;
    ctx.font = `${Math.max(11, Math.round(11 * (window.devicePixelRatio || 1)))}px sans-serif`;
    ctx.fillText(String(title), L, T + 12);
  }

  if (Number.isFinite(Number(viewState.hoverLocalIdx))) {
    const hi = Math.trunc(Number(viewState.hoverLocalIdx));
    if (hi >= 0 && hi < n) {
      const x = xOf(hi);
      ctx.strokeStyle = border;
      ctx.setLineDash([3, 3]);
      ctx.beginPath();
      ctx.moveTo(x, T);
      ctx.lineTo(x, T + ph);
      ctx.stroke();
      ctx.setLineDash([]);
    }
  }
}

function choosePalette() {
  const cs = getComputedStyle(document.body);
  const c1 = cs.getPropertyValue("--accent").trim() || "#3b82f6";
  const c2 = cs.getPropertyValue("--success").trim() || "#10b981";
  const c3 = cs.getPropertyValue("--danger").trim() || "#ef4444";
  const c4 = cs.getPropertyValue("--text-dim").trim() || "#94a3b8";
  return [c1, c2, c3, c4, "#a855f7", "#f59e0b", "#14b8a6"];
}

function computeWarmupBars(keys, params) {
  let mx = 0;
  for (const k of keys || []) {
    if (typeof k !== "string") continue;
    if (k.startsWith("ma_")) mx = Math.max(mx, Number(k.slice(3)) || 0);
    else if (k.startsWith("rsi_")) mx = Math.max(mx, (Number(k.slice(4)) || 0) + 1);
    else if (k === "ema") mx = Math.max(mx, Number(params.emaPeriod || 20) || 0);
    else if (k === "boll_up") mx = Math.max(mx, Number(params.bollPeriod || 20) || 0);
    else if (k === "boll_down") mx = Math.max(mx, Number(params.bollDownPeriod || 20) || 0);
    else if (k === "supertrend") mx = Math.max(mx, Number(params.superAtrPeriod || 10) || 0);
    else if (k === "kdj_k" || k === "kdj_d" || k === "kdj_j") mx = Math.max(mx, Number(params.kdjN || 9) || 0);
    else if (k === "obv_ma") mx = Math.max(mx, Number(params.obvMaPeriod || 20) || 0);
    else if (k === "stoch_rsi_k" || k === "stoch_rsi_d") mx = Math.max(mx, Number(params.stochRsiP || 14) || 0);
  }
  const warmup = Math.min(600, Math.max(80, mx * 2 + 10));
  return warmup;
}

async function loadKlineFromApi(market, symbol, tail) {
  const u = `./api/kline?market=${encodeURIComponent(market)}&symbol=${encodeURIComponent(symbol)}&tail=${encodeURIComponent(String(tail))}`;
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

function computeSeriesForKeys(row, keys, params) {
  const s = row.series || {};
  const close = Array.isArray(s.close) ? s.close : [];
  const high = Array.isArray(s.high) ? s.high : [];
  const low = Array.isArray(s.low) ? s.low : [];
  const volume = Array.isArray(s.volume) ? s.volume : [];
  const open = Array.isArray(s.open) ? s.open : [];
  const quote_volume = Array.isArray(s.quote_volume) ? s.quote_volume : [];
  const palette = choosePalette();
  let pi = 0;

  const overlays = [];
  const indicators = [];
  const done = new Set();

  const addOverlay = (key, name, series) => {
    overlays.push({ key, name, series, color: palette[pi++ % palette.length] });
  };
  const addInd = (key, name, series) => {
    indicators.push({ key, name, series, color: palette[pi++ % palette.length] });
  };

  for (const key of keys) {
    if (done.has(key)) continue;
    if (key.startsWith("expr_")) {
      const id = key.slice(5);
      const cfg = viewState.customFactorsById ? viewState.customFactorsById[id] : null;
      const name = cfg && (cfg.name || cfg.template || cfg.expr) ? String(cfg.name || cfg.template || cfg.expr) : key;
      try {
        if (typeof tokenizeExpr === "function" && typeof parseExprTokens === "function" && typeof evalAst === "function" && typeof seriesFrom === "function") {
          const template = cfg ? String(cfg.template || cfg.expr || "") : "";
          const expr = (cfg && typeof expandTemplate === "function") ? expandTemplate(template, cfg.params || []) : template;
          const tokens = tokenizeExpr(expr);
          const ast = parseExprTokens(tokens);
          const seriesCtx = { open, high, low, close, volume, quote_volume };
          const latest = {
            open: lastFinite(open),
            high: lastFinite(high),
            low: lastFinite(low),
            close: lastFinite(close),
            volume: lastFinite(volume),
            quote_volume: lastFinite(quote_volume),
          };
          const v = evalAst(ast, { series: seriesCtx, latest });
          const out = seriesFrom(v, close.length);
          addInd(key, name, out);
        }
      } catch {}
      continue;
    }
    if (key.startsWith("ma_")) {
      const p = Number(key.slice(3));
      addOverlay(key, `MA(${p})`, smaSeries(close, p));
      continue;
    }
    if (key.startsWith("rsi_")) {
      const p = Number(key.slice(4));
      addInd(key, `RSI(${p})`, rsiSeries(close, p));
      continue;
    }
    if (key === "ema") {
      addOverlay(key, `EMA(${params.emaPeriod || 20})`, emaSeries(close, params.emaPeriod || 20));
      continue;
    }
    if (key === "boll_up") {
      const p = params.bollPeriod || 20;
      const k = params.bollStd || 2;
      const ma = smaSeries(close, p);
      const sd = rollingStdSeries(close, p);
      const out = ma.map((v, i) => (v === null || sd[i] === null ? null : v + k * sd[i]));
      addOverlay(key, `BOLLUP(${p},${k})`, out);
      continue;
    }
    if (key === "boll_down") {
      const p = params.bollDownPeriod || 20;
      const k = params.bollDownStd || 2;
      const ma = smaSeries(close, p);
      const sd = rollingStdSeries(close, p);
      const out = ma.map((v, i) => (v === null || sd[i] === null ? null : v - k * sd[i]));
      addOverlay(key, `BOLLDOWN(${p},${k})`, out);
      continue;
    }
    if (key === "supertrend") {
      const p = params.superAtrPeriod || 10;
      const m = params.superMult || 3;
      addOverlay(key, `SUPER(${p},${m})`, supertrendSeries(high, low, close, p, m));
      continue;
    }
    if (key === "kdj_k" || key === "kdj_d" || key === "kdj_j") {
      const n = params.kdjN || 9;
      const m1 = params.kdjM1 || 3;
      const m2 = params.kdjM2 || 3;
      const { K, D, J } = kdjSeries(high, low, close, n, m1, m2);
      if (keys.includes("kdj_k")) {
        addInd("kdj_k", `KDJ-K(${n},${m1},${m2})`, K);
        done.add("kdj_k");
      }
      if (keys.includes("kdj_d")) {
        addInd("kdj_d", `KDJ-D(${n},${m1},${m2})`, D);
        done.add("kdj_d");
      }
      if (keys.includes("kdj_j")) {
        addInd("kdj_j", `KDJ-J(${n},${m1},${m2})`, J);
        done.add("kdj_j");
      }
      continue;
    }
    if (key === "obv" || key === "obv_ma") {
      const ob = obvSeries(close, volume);
      if (keys.includes("obv")) {
        addInd("obv", "OBV", ob);
        done.add("obv");
      }
      if (keys.includes("obv_ma")) {
        addInd("obv_ma", `OBV_MA(${params.obvMaPeriod || 20})`, smaSeries(ob, params.obvMaPeriod || 20));
        done.add("obv_ma");
      }
      continue;
    }
    if (key === "stoch_rsi_k" || key === "stoch_rsi_d") {
      const p = params.stochRsiP || 14;
      const k = params.stochRsiK || 14;
      const sk = params.stochRsiSmK || 3;
      const sd = params.stochRsiSmD || 3;
      const { K, D } = stochRsiSeries(close, p, k, sk, sd);
      if (keys.includes("stoch_rsi_k")) {
        addInd("stoch_rsi_k", `StochRSI-K(${p},${k})`, K);
        done.add("stoch_rsi_k");
      }
      if (keys.includes("stoch_rsi_d")) {
        addInd("stoch_rsi_d", `StochRSI-D(${p},${k})`, D);
        done.add("stoch_rsi_d");
      }
      continue;
    }
  }

  // 成交量作为可管理的指标
  if (!done.has('volume')) {
    addInd('volume', '成交量', volume);
    done.add('volume');
  }

  return { overlays, indicators };
}

async function render() {
  applyThemeFromStorage();
  const candleEl = $("candleCanvas");
  const candleCtx = ensureCanvas(candleEl);
  if (!candleCtx) return;

  try {
    showAlert("");
    const q = parseQuery();
    const params = loadParams();
    const plotKeys = loadPlotKeys();
    const basics = new Set(["rank", "symbol", "market", "dt_display", "close"]);
    const keys = plotKeys.filter((k) => !basics.has(k));

    const selectedKey = q.key || localStorage.getItem(selectedKeyStorageKey) || "";
    if (!selectedKey) {
      $("klineTitle").textContent = "K线";
      $("klineSub").textContent = "未选择币种";
      showAlert("未选择币种：请从主页面点击币种名称进入 K 线页");
      drawEmpty(candleCtx, "未选择币种");
      viewState.row = null;
      updateBinanceLink(null);
      viewState.indicatorPanels = buildIndicatorStack([]);
      drawFromState();
      return;
    }

    const latest = await loadJsonNoCache("./data/latest.json");
    const rows = Array.isArray(latest.results) ? latest.results : [];
    viewState.latestRows = rows;
    const lastPicks = loadLastPicks();
    const rankMap = new Map();
    const pickArr = lastPicks && Array.isArray(lastPicks.rows) ? lastPicks.rows : [];
    for (const it of pickArr) {
      const k = pickKey(it);
      if (!k || k === "|") continue;
      const rr = Number(it && (it.rank ?? it._rank));
      if (Number.isFinite(rr)) rankMap.set(k, rr);
    }
    viewState.pickRankMap = rankMap;
    viewState.pickRows = rows;
    viewState.pickLabel = rankMap.size ? `主页榜${rankMap.size}` : "";
    const row = rows.find((r) => `${String(r.market)}|${String(r.symbol)}` === selectedKey);
    const searchEl = document.getElementById("pickSearch");
    if (searchEl && !searchEl.__wired) {
      searchEl.__wired = true;
      searchEl.addEventListener("input", () => {
        viewState.pickQuery = String(searchEl.value || "");
        renderPickList(viewState.pickRows, viewState.key || selectedKey);
      });
    }
    if (searchEl && String(searchEl.value || "") !== String(viewState.pickQuery || "")) searchEl.value = String(viewState.pickQuery || "");
    renderPickList(rows, selectedKey);
    if (!row) {
      $("klineTitle").textContent = "K线";
      $("klineSub").textContent = "未找到该币种（可能不在当前筛选结果或市场切换）";
      showAlert("未找到该币种：请先在主页面刷新并确保该币种在结果中");
      drawEmpty(candleCtx, "未找到该币种");
      viewState.row = null;
      updateBinanceLink(null);
      viewState.indicatorPanels = buildIndicatorStack([]);
      drawFromState();
      return;
    }
    $("klineTitle").textContent = `${stripQuote(row.symbol)}（${marketLabel(row.market)}）`;
    updateBinanceLink(row);
    const sum = latest.summary || {};
    const bh = Number((latest.config || {}).bar_hours || 1) || 1;
    let latestText = "";
    if (sum.latest_dt_close) {
      latestText = fmtDt(sum.latest_dt_close);
    } else if (sum.latest_dt_display) {
      const ms0 = Date.parse(String(sum.latest_dt_display));
      latestText = Number.isFinite(ms0) ? fmtDt(ms0 + bh * 3600 * 1000) : fmtDt(sum.latest_dt_display);
    } else {
      latestText = "";
    }
    $("klineSub").textContent = `生成：${fmtDt(sum.generated_at)} ｜ 最新：${latestText}`;

    // 切换币种时保存旧币种绘图，加载新币种绘图
    const prevKey = viewState.key;
    if (prevKey && prevKey !== selectedKey) saveDrawingsForKey(prevKey);
    viewState.key = selectedKey;
    viewState.latestSummary = latest.summary || {};
    viewState.barHours = Number((latest.config || {}).bar_hours || 1) || 1;
    viewState.params = params;
    viewState.plotKeys = keys;
    viewState.hoverLocalIdx = null;
    viewState.customFactorsById = loadCustomFactorsById();
    viewState.hiddenKeys = loadHiddenKeys();

    const tailDisplay = loadTailSetting(Number((latest.config || {}).tail_len || 720));
    const warmup = computeWarmupBars(keys, params);
    const tailApi = Math.min(3650, tailDisplay + warmup);

    const api = await loadKlineFromApi(String(row.market), String(row.symbol), tailApi);
    const s = api.series || {};
    const close = Array.isArray(s.close) ? s.close : [];
    const n0 = close.length || 0;
    if (!n0) throw new Error("k线数据为空");
    const lastClose0 = Number(close[n0 - 1]);
    const lastCloseText = Number.isFinite(lastClose0) ? ` ${fmtNum(lastClose0)}` : "";
    $("klineTitle").textContent = `${stripQuote(row.symbol)}（${marketLabel(row.market)}）${lastCloseText}`;

    const dt = Array.isArray(api.dt) ? api.dt : [];
    const timesMs = dt.map((x) => parseIsoToMs(x));
    viewState.dt = dt;
    viewState.timesMs = timesMs;

    const row2 = { ...row, series: s };
    viewState.row = row2;
    viewState.n0 = n0;

    viewState.viewN = Math.min(tailDisplay, n0);
    viewState.offset = 0;

    const { overlays, indicators } = computeSeriesForKeys(row2, keys, params);
    const placement = loadPlacement();
    const mainOverlays = [];
    const panelMap = new Map();
    const maxPanels = 4;
    const allItems = [
      ...overlays.map((x) => ({ ...x, kind: "overlay" })),
      ...indicators.map((x) => ({ ...x, kind: "indicator" })),
    ];
    for (const it of allItems) {
      const k = String(it.key || "");
      const v0 = placement[k];
      const v = Number.isFinite(Number(v0)) ? Math.trunc(Number(v0)) : null;
      // volume 特殊处理：placement=0 表示在主图底部柱状图，不作为 overlay 折线
      if (k === 'volume') {
        if (v !== null && v !== 0 && v !== -1) {
          // 移到副图
          const pid = Math.max(1, Math.min(maxPanels, v));
          if (!panelMap.has(pid)) panelMap.set(pid, []);
          panelMap.get(pid).push(it);
        }
        // v===0 或 null → 主图柱状图（由 showVolumeBar 控制），不入 mainOverlays
        // v===-1 → 隐藏
        continue;
      }
      const def = it.kind === "overlay" ? 0 : 1;
      const where = v === null ? def : v;
      if (where === -1) continue;
      if (where === 0) {
        mainOverlays.push(it);
        continue;
      }
      const pid = Math.max(1, Math.min(maxPanels, where));
      if (!panelMap.has(pid)) panelMap.set(pid, []);
      panelMap.get(pid).push(it);
    }
    viewState.overlaysAll = mainOverlays;
    const panels = Array.from(panelMap.entries())
      .sort((a, b) => a[0] - b[0])
      .map(([panelId, items]) => ({ panelId, title: `副图${panelId}`, items }));
    viewState.indicatorPanels = buildIndicatorStack(panels);
    attachInteractions($("candleCanvas"));
    for (const p of viewState.indicatorPanels) attachInteractions(p.canvas);
    buildIndicatorSettings(allItems);
    updateChipBar(document.getElementById("candleChipBar"), viewState.overlaysAll);
    for (const p of viewState.indicatorPanels || []) {
      if (p && p.chipbar) updateChipBar(p.chipbar, p.items || []);
    }

    computeWindow();
    loadDrawingsForKey(selectedKey);
    drawFromState();
  } catch (e) {
    $("klineTitle").textContent = "K线";
    $("klineSub").textContent = `绘制失败：${String(e && e.message ? e.message : e)}`;
    showAlert(String(e && e.stack ? e.stack : e));
    drawEmpty(candleCtx, "绘制失败");
    viewState.row = null;
    viewState.indicatorPanels = buildIndicatorStack([]);
  }
}

function drawFromState() {
  const candleCtx = ensureCanvas($("candleCanvas"));
  if (!candleCtx) return;
  if (!viewState.row) {
    drawEmpty(candleCtx, "暂无数据");
    const panels = viewState.indicatorPanels || [];
    for (const p of panels) {
      const ctx = ensureCanvas(p.canvas);
      if (ctx) drawEmpty(ctx, "暂无数据");
    }
    return;
  }
  computeWindow();
  const start = viewState.start;
  const end = viewState.end;
  const n = viewState.n;

  const labelAt = (i) => fmtTs(barTimeMs(start + i));
  const overlays = (viewState.overlaysAll || [])
    .filter((x) => !isKeyHidden(x && x.key ? x.key : ""))
    .map((x) => ({ ...x, series: (x.series || []).slice(start, end + 1) }));

  // 成交量在主图时显示柱状图，在副图或隐藏时不显示
  const placement = loadPlacement();
  const volPlacement = placement['volume'];
  const volWhere = Number.isFinite(Number(volPlacement)) ? Math.trunc(Number(volPlacement)) : 0;
  const showVolumeBar = (volWhere === 0) && !isKeyHidden('volume');

  drawCandles(candleCtx, viewState.row, overlays, start, n, end, labelAt, viewState.hoverLocalIdx, showVolumeBar);

  const panels = viewState.indicatorPanels || [];
  for (const p of panels) {
    const ctx = ensureCanvas(p.canvas);
    if (!ctx) continue;
    const groups = (p.items || [])
      .filter((x) => !isKeyHidden(x && x.key ? x.key : ""))
      .map((x) => ({ ...x, series: (x.series || []).slice(start, end + 1) }));
    drawIndicators(ctx, groups, n, labelAt, p.title);
  }
  updateStatsPanel();
}

function buildIndicatorStack(panels) {
  const host = document.getElementById("indicatorStack");
  if (!host) return [];
  const panelWrap = document.getElementById("indicatorPanel");
  const ps0 = Array.isArray(panels) ? panels : [];
  const total = ps0.reduce((a, p) => a + ((p && p.items && p.items.length) || 0), 0);
  if (panelWrap) {
    if (total <= 0) panelWrap.classList.add("hidden");
    else panelWrap.classList.remove("hidden");
  }
  host.innerHTML = "";
  if (total <= 0) return [];
  const list = ps0.length ? ps0 : [{ panelId: 1, title: "副图1", items: [] }];
  host.style.gridTemplateRows = `repeat(${list.length}, 1fr)`;
  const out = [];
  for (const p of list) {
    const wrap = document.createElement("div");
    wrap.className = "kline-indicator-item";
    const chip = document.createElement("div");
    chip.className = "kline-chipbar";
    chip.setAttribute("data-panel", String(p.panelId));
    wrap.appendChild(chip);
    const c = document.createElement("canvas");
    c.className = "kline-canvas";
    c.setAttribute("data-panel", String(p.panelId));
    c.height = 180;
    wrap.appendChild(c);
    host.appendChild(wrap);
    out.push({ ...p, canvas: c, chipbar: chip });
  }
  return out;
}

function setIndicatorsOpen(open) {
  const box = document.getElementById("indicatorSettings");
  if (!box) return;
  if (open) box.classList.remove("hidden");
  else box.classList.add("hidden");
}

function buildIndicatorSettings(allItems) {
  const host = document.getElementById("indicatorList");
  if (!host) return;
  host.innerHTML = "";
  const placement = loadPlacement();
  const maxPanels = 4;

  const items = Array.isArray(allItems) ? allItems : [];
  for (const it of items) {
    const k = String(it.key || "");
    if (!k) continue;
    const row = document.createElement("div");
    row.className = "kline-settings-row";

    const name = document.createElement("div");
    name.className = "kline-settings-name";
    name.textContent = String(it.name || k);

    const sel = document.createElement("select");
    sel.className = "select";

    const def = it.kind === "overlay" ? 0 : 1;
    const cur0 = placement[k];
    const cur = Number.isFinite(Number(cur0)) ? Math.trunc(Number(cur0)) : def;

    const addOpt = (v, label) => {
      const o = document.createElement("option");
      o.value = String(v);
      o.textContent = label;
      if (v === cur) o.selected = true;
      sel.appendChild(o);
    };

    addOpt(0, "主图");
    for (let i = 1; i <= maxPanels; i++) addOpt(i, `副图${i}`);
    addOpt(-1, "隐藏");

    sel.addEventListener("change", () => {
      const v = Math.trunc(Number(sel.value));
      placement[k] = Number.isFinite(v) ? v : def;
      savePlacement(placement);
      render();
    });

    row.appendChild(name);
    row.appendChild(sel);
    host.appendChild(row);
  }
}

function showTooltip(text, x, y) {
  const el = document.getElementById("klineTooltip");
  if (!el) return;
  if (!text) {
    el.classList.add("hidden");
    el.innerHTML = "";
    return;
  }
  el.classList.remove("hidden");
  el.innerHTML = String(text);
  const pad = 12;
  const rect = el.getBoundingClientRect();
  const nx = Math.max(pad, Math.min(window.innerWidth - rect.width - pad, x));
  const ny = Math.max(pad, Math.min(window.innerHeight - rect.height - pad, y));
  el.style.left = `${Math.round(nx)}px`;
  el.style.top = `${Math.round(ny)}px`;
}

function panelById(panelId) {
  const ps = viewState.indicatorPanels || [];
  const pid = Math.trunc(Number(panelId));
  for (const p of ps) {
    if (!p) continue;
    if (Math.trunc(Number(p.panelId)) === pid) return p;
  }
  return null;
}

function getAllIndicatorItems() {
  const out = [];
  for (const p of viewState.indicatorPanels || []) {
    for (const it of (p && p.items) || []) {
      if (isKeyHidden(it && it.key ? it.key : "")) continue;
      out.push({ panel: p, item: it });
    }
  }
  return out;
}

function buildMainHoverText(gi) {
  const ms = barTimeMs(gi);
  const s = (viewState.row || {}).series || {};
  const o = Number((s.open || [])[gi]);
  const h = Number((s.high || [])[gi]);
  const l = Number((s.low || [])[gi]);
  const c = Number((s.close || [])[gi]);
  const vol = Number((s.volume || [])[gi]);
  const pct = Number.isFinite(o) && o !== 0 && Number.isFinite(c) ? ((c / o - 1) * 100) : null;
  const isUp = pct === null ? true : pct >= 0;
  const pctColor = isUp ? 'var(--success,#10b981)' : 'var(--danger,#ef4444)';
  const pctStr = pct === null ? '-' : (pct >= 0 ? '+' : '') + pct.toFixed(2) + '%';

  const esc = (v) => String(v).replace(/</g,'&lt;').replace(/>/g,'&gt;');
  const row = (label, value, color) =>
    `<div class="ktt-row"><span class="ktt-label">${esc(label)}</span><span class="ktt-value"${color ? ` style="color:${color}"` : ''}>${esc(value)}</span></div>`;

  let html = `<div class="ktt-head"><span class="ktt-time">${esc(fmtTs(ms))}</span><span class="ktt-pct" style="color:${pctColor}">${esc(pctStr)}</span></div>`;
  html += `<div class="ktt-divider"></div>`;
  html += `<div class="ktt-ohlc">`;
  html += `<div class="ktt-ohlc-item"><span class="ktt-label">开</span><span class="ktt-value">${fmtNum(o)}</span></div>`;
  html += `<div class="ktt-ohlc-item"><span class="ktt-label">高</span><span class="ktt-value" style="color:var(--success,#10b981)">${fmtNum(h)}</span></div>`;
  html += `<div class="ktt-ohlc-item"><span class="ktt-label">低</span><span class="ktt-value" style="color:var(--danger,#ef4444)">${fmtNum(l)}</span></div>`;
  html += `<div class="ktt-ohlc-item"><span class="ktt-label">收</span><span class="ktt-value" style="color:${pctColor}">${fmtNum(c)}</span></div>`;
  html += `</div>`;
  if (Number.isFinite(vol) && vol > 0) {
    html += `<div class="ktt-divider"></div>`;
    html += row('成交量', fmtAxisNum(vol), null);
  }
  // 主图指标
  const ovItems = (viewState.overlaysAll || []).filter(it => !isKeyHidden(it && it.key ? it.key : ''));
  if (ovItems.length > 0) {
    html += `<div class="ktt-divider"></div>`;
    for (const it of ovItems) {
      const v = Number((it && it.series ? it.series : [])[gi]);
      html += row(String(it.name || it.key), Number.isFinite(v) ? fmtNum(v) : '-', it.color || null);
    }
  }
  // 副图指标
  const indItems = getAllIndicatorItems();
  if (indItems.length > 0) {
    html += `<div class="ktt-divider"></div>`;
    for (const x of indItems) {
      const it = x.item;
      if (isKeyHidden(it && it.key ? it.key : '')) continue;
      const v = Number((it && it.series ? it.series : [])[gi]);
      const prefix = x.panel ? String(x.panel.title || `副图${x.panel.panelId}`) : '副图';
      html += row(`${prefix} ${String(it.name || it.key)}`, Number.isFinite(v) ? fmtNum(v) : '-', it.color || null);
    }
  }
  return html;
}

function buildPanelHoverText(panelId, gi, mouseY, canvas) {
  const p = panelById(panelId);
  if (!p) return "";
  const items = (p.items || []).filter((x) => !isKeyHidden(x && x.key ? x.key : "")).slice();
  const lines = [];
  for (const it of items) {
    const v = Number((it.series || [])[gi]);
    lines.push(`${String(it.name || it.key)} ${Number.isFinite(v) ? fmtNum(v) : "-"}`);
  }
  return lines.join("\n");
}

function attachInteractions(canvas) {
  if (!canvas) return;
  if (canvas.dataset && canvas.dataset.klineBound === "1") return;
  if (canvas.dataset) canvas.dataset.klineBound = "1";

  const getPlotMetrics = () => {
    const w = canvas.width;
    const L = axisL;
    const R = 10;
    const pw = Math.max(10, w - L - R);
    return { L, pw };
  };

  const setOffset = (v) => {
    viewState.offset = v;
    scheduleDraw();
  };

  const setViewN = (v) => {
    viewState.viewN = v;
    scheduleDraw();
  };

  canvas.addEventListener("pointerdown", (e) => {
    if (!viewState.row || !viewState.n0) return;
    // 绘图工具激活时，不触发拖拽
    if (drawState.tool !== 'cursor' && drawState.tool !== 'info') return;
    viewState.dragging = true;
    viewState.dragStartX = e.clientX;
    viewState.dragStartOffset = viewState.offset;
    try {
      canvas.setPointerCapture(e.pointerId);
    } catch {}
  });

  canvas.addEventListener("pointermove", (e) => {
    if (!viewState.row) return;

    if (viewState.dragging) {
      const { pw } = getPlotMetrics();
      const n = Math.max(1, viewState.n);
      const barPerPx = n / Math.max(10, pw / (window.devicePixelRatio || 1));
      const dx = e.clientX - viewState.dragStartX;
      const deltaBars = Math.round(dx * barPerPx);
      const n0 = viewState.n0;
      const maxOffset = Math.max(0, n0 - viewState.viewN);
      const next = clampInt(viewState.dragStartOffset + deltaBars, 0, maxOffset);
      setOffset(next);
      return;
    }

    const rect = canvas.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;
    const { L, pw } = getPlotMetrics();
    const pxL = L / dpr;
    const pxPw = pw / dpr;
    const x = e.clientX - rect.left;
    const n = Math.max(1, viewState.n);
    const step = pxPw / n;
    const rel = x - pxL;
    if (rel < 0 || rel > pxPw) {
      viewState.hoverLocalIdx = null;
      showTooltip("", 0, 0);
      scheduleDraw();
      return;
    }
    const i = clampInt(Math.floor(rel / step), 0, n - 1);
    if (viewState.hoverLocalIdx !== i) {
      viewState.hoverLocalIdx = i;
      scheduleDraw();
    }
    const gi = viewState.start + i;
    if (canvas.id === "candleCanvas") {
      // 划线工具激活时不显示单根K线详情
      if (drawState.tool !== 'cursor' && drawState.tool !== 'info') {
        showTooltip('', 0, 0);
      } else {
        showTooltip(buildMainHoverText(gi), e.clientX + 12, e.clientY + 12);
      }
      updateStatsPanel();
      return;
    }
    const pid = canvas.dataset ? canvas.dataset.panel : "";
    const my = (e.clientY - rect.top) * dpr;
    showTooltip(buildPanelHoverText(pid, gi, my, canvas), e.clientX + 12, e.clientY + 12);
  });

  const endDrag = () => {
    viewState.dragging = false;
  };

  canvas.addEventListener("pointerup", endDrag);
  canvas.addEventListener("pointercancel", endDrag);
  canvas.addEventListener("pointerleave", endDrag);
  canvas.addEventListener("pointerleave", () => {
    if (!viewState.dragging) {
      viewState.hoverLocalIdx = null;
      showTooltip("", 0, 0);
      scheduleDraw();
    }
  });

  canvas.addEventListener("wheel", (e) => {
    if (!viewState.row || !viewState.n0) return;
    e.preventDefault();
    const rect = canvas.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;
    const { L, pw } = getPlotMetrics();
    const pxL = L / dpr;
    const pxPw = pw / dpr;
    const x = e.clientX - rect.left;
    const ratio0 = (x - pxL) / Math.max(10, pxPw);
    const ratio = Math.max(0, Math.min(1, ratio0));

    const n0 = viewState.n0;
    const curN = viewState.viewN;
    const zoomIn = e.deltaY < 0;
    const nextN0 = zoomIn ? Math.round(curN * 0.85) : Math.round(curN * 1.15);
    const nextN = clampInt(nextN0, 30, n0);
    if (nextN === curN) return;

    const curEnd = viewState.end;
    const curStart = viewState.start;
    const curSpan = Math.max(1, viewState.n - 1);
    const cursorIdx = curStart + ratio * curSpan;
    const nextStart = Math.round(cursorIdx - ratio * Math.max(1, nextN - 1));
    const maxStart = Math.max(0, n0 - nextN);
    const st = clampInt(nextStart, 0, maxStart);
    const endIdx = st + nextN - 1;
    const off = (n0 - 1) - endIdx;

    viewState.viewN = nextN;
    viewState.offset = clampInt(off, 0, Math.max(0, n0 - nextN));
    scheduleDraw();
  }, { passive: false });
}

function initKlinePage() {
  wireGlobalErrorHandler();
  const back = $("btnBack");
  const reload = $("btnReload");
  const btnIndicators = $("btnIndicators");
  const btnCloseIndicators = $("btnCloseIndicators");
  const btnResetView = $("btnResetView");
  const tailSelect = $("tailSelect");
  if (back) {
    back.addEventListener("click", () => {
      if (window.history.length > 1) {
        window.history.back();
        return;
      }
      const ref = document.referrer || "";
      try {
        const u = ref ? new URL(ref) : null;
        if (u && u.origin === window.location.origin) {
          window.location.href = ref;
          return;
        }
      } catch {}
      window.location.href = "./index.html";
    });
  }
  if (reload) reload.addEventListener("click", () => render());
  if (tailSelect) {
    const v0 = loadTailSetting();
    tailSelect.value = String(v0);
    tailSelect.addEventListener("change", () => {
      const v = Math.trunc(Number(tailSelect.value));
      const next = v === 360 || v === 720 || v === 1440 || v === 2160 || v === 3650 ? v : 360;
      saveTailSetting(next);
      showAlert(`已选择显示 ${next} 根K线；点击“重新加载”后生效。`);
    });
  }
  if (btnIndicators) btnIndicators.addEventListener("click", () => setIndicatorsOpen(true));
  if (btnCloseIndicators) btnCloseIndicators.addEventListener("click", () => setIndicatorsOpen(false));
  const settings = document.getElementById("indicatorSettings");
  if (settings) settings.addEventListener("click", (e) => {
    if (e && e.target === settings) setIndicatorsOpen(false);
  });
  if (btnResetView) {
    btnResetView.addEventListener("click", () => {
      viewState.viewN = loadTailSetting();
      viewState.offset = 0;
      viewState.hoverLocalIdx = null;
      showTooltip("", 0, 0);
      scheduleDraw();
    });
  }
  attachInteractions($("candleCanvas"));
  try {
    if (window.ResizeObserver) {
      const ro = new ResizeObserver(() => {
        if (viewState.row) scheduleDraw();
      });
      const candlePanel = document.querySelector(".kline-candle-panel");
      const indPanel = document.getElementById("indicatorPanel");
      if (candlePanel) ro.observe(candlePanel);
      if (indPanel) ro.observe(indPanel);
    }
  } catch {}
  window.addEventListener("resize", () => {
    if (viewState.row) scheduleDraw();
    else render();
  });
  window.addEventListener("storage", (e) => {
    if (e && e.key === themeStorageKey) {
      applyThemeFromStorage();
      if (viewState.row) scheduleDraw();
      else render();
    }
  });
  applyThemeFromStorage();
  requestAnimationFrame(() => render());
  setTimeout(() => render(), 250);
}

// ===================== 工具栏绘图系统 =====================
const DRAW_COLORS = [
  '#3b82f6','#10b981','#ef4444','#f59e0b','#a855f7',
  '#14b8a6','#f97316','#ec4899','#ffffff','#94a3b8',
  '#fbbf24','#34d399','#60a5fa','#f87171','#c084fc',
  '#fb923c','#e2e8f0','#475569','#1e293b','#0f172a'];

const FIB_LEVELS = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0];
const FIB_EXT_LEVELS = [0, 0.618, 1.0, 1.382, 1.618, 2.0, 2.618];
const FIB_COLORS = ['#ef4444','#f59e0b','#10b981','#3b82f6','#a855f7','#14b8a6','#94a3b8'];

const WAVE_LABELS_IMPULSE = ['①','②','③','④','⑤'];  // 推进浪
const WAVE_LABELS_CORRECTIVE = ['A','B','C'];          // 调整浪
const WAVE_LABEL_SETS = [WAVE_LABELS_IMPULSE, WAVE_LABELS_CORRECTIVE];

const drawState = {
  tool: 'cursor',
  color: '#3b82f6',
  drawings: [],        // { type, points:[{x,y,gi,price},...], color, text? }
  drawingsByKey: {},   // { [symbolKey]: drawings[] } 按币种隔离
  drawing: false,
  erasing: false,
  tempStart: null,
  drawCanvas: null,
  drawCtx: null,
  infoVisible: false,
  statsVisible: false,  // 右上角统计面板，默认隐藏，点指标信息框按钮开启
  // 波浪标注临时点集
  wavePoints: [],
  waveLabelSet: 0,     // 0=推进浪①~⑤, 1=调整浪A~C
};

// ---- 按币种隔离绘图 ----
function saveDrawingsForKey(key) {
  if (!key) return;
  drawState.drawingsByKey[key] = drawState.drawings.slice();
}
function loadDrawingsForKey(key) {
  if (!key) { drawState.drawings = []; return; }
  drawState.drawings = (drawState.drawingsByKey[key] || []).slice();
  drawState.wavePoints = [];
  drawState.drawing = false;
  drawState.tempStart = null;
  drawState.tempEnd = null;
  redrawDrawings();
}

// ---- 坐标映射 ----
function canvasToPlot(canvas, clientX, clientY) {
  const rect = canvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  const L = axisL;
  const R = 10;
  const w = canvas.width;
  const pw = Math.max(10, w - L - R);
  const n = Math.max(1, viewState.n);
  const stepX = pw / n;
  const x = (clientX - rect.left) * dpr;
  const y = (clientY - rect.top) * dpr;
  const relX = x - L;
  const localI = Math.round(relX / stepX - 0.5);
  const gi = viewState.start + Math.max(0, Math.min(n - 1, localI));
  const s = (viewState.row && viewState.row.series) ? viewState.row.series : {};
  const closes = s.close || [];
  const highs = s.high || [];
  const lows = s.low || [];
  // Y price mapping
  const candle = document.getElementById('candleCanvas');
  if (!candle) return { gi, price: null, px: x, py: y, localI };
  const cw = candle.width;
  const ch = candle.height;
  const T = 10, B = 22;
  const cph = ch - T - B;
  // recompute yMin/yMax from current view
  let minP = Infinity, maxP = -Infinity;
  const st = viewState.start, end = viewState.end;
  for (let i = st; i <= end; i++) {
    const hi = Number((highs)[i]); const lo = Number((lows)[i]);
    if (Number.isFinite(hi)) maxP = Math.max(maxP, hi);
    if (Number.isFinite(lo)) minP = Math.min(minP, lo);
  }
  if (!Number.isFinite(minP)) { return { gi, price: null, px: x, py: y, localI }; }
  const pad = (maxP - minP) * 0.06;
  const yMax = maxP + pad, yMin = minP - pad;
  const price = yMax - ((y - T) / cph) * (yMax - yMin);
  return { gi, price, px: x, py: y, localI };
}

function priceToY(price, canvasH) {
  const s = (viewState.row && viewState.row.series) ? viewState.row.series : {};
  const highs = s.high || [], lows = s.low || [];
  const st = viewState.start, end = viewState.end;
  let minP = Infinity, maxP = -Infinity;
  for (let i = st; i <= end; i++) {
    const hi = Number(highs[i]), lo = Number(lows[i]);
    if (Number.isFinite(hi)) maxP = Math.max(maxP, hi);
    if (Number.isFinite(lo)) minP = Math.min(minP, lo);
  }
  if (!Number.isFinite(minP)) return null;
  const pad = (maxP - minP) * 0.06;
  const yMax = maxP + pad, yMin = minP - pad;
  const T = 10, B = 22;
  const ph = canvasH - T - B;
  return T + ((yMax - price) / (yMax - yMin)) * ph;
}

function giToX(gi, canvasW) {
  const L = axisL, R = 10;
  const pw = Math.max(10, canvasW - L - R);
  const n = Math.max(1, viewState.n);
  const stepX = pw / n;
  const localI = gi - viewState.start;
  return L + localI * stepX + stepX / 2;
}

// ---- 绘制所有 drawings ----
function redrawDrawings() {
  const dc = drawState.drawCtx;
  const canvas = drawState.drawCanvas;
  if (!dc || !canvas) return;
  const w = canvas.width, h = canvas.height;
  dc.clearRect(0, 0, w, h);
  const dpr = window.devicePixelRatio || 1;
  dc.save();
  dc.lineWidth = 1.5 * dpr;
  for (const d of drawState.drawings) {
    renderDrawing(dc, d, w, h, dpr, false);
  }
  // 波浪工具预览已点节点
  if (drawState.tool === 'wave' && drawState.wavePoints.length > 0) {
    const fake = { type: 'wave', points: drawState.wavePoints, color: drawState.color, labelSet: drawState.waveLabelSet };
    renderDrawing(dc, fake, w, h, dpr, true);
  }
  // temp drawing preview
  if (drawState.drawing && drawState.tempStart && drawState.tempEnd) {
    const fake = { type: drawState.tool, points: [drawState.tempStart, drawState.tempEnd], color: drawState.color };
    renderDrawing(dc, fake, w, h, dpr, true);
  }
  dc.restore();
}

function renderDrawing(dc, d, w, h, dpr, isPreview) {
  if (!d || !d.points || d.points.length < 1) return;
  dc.strokeStyle = d.color || '#3b82f6';
  dc.fillStyle = d.color || '#3b82f6';
  dc.lineWidth = 1.5 * dpr;
  if (isPreview) dc.globalAlpha = 0.72;
  else dc.globalAlpha = 1.0;

  const p0 = d.points[0];
  const p1 = d.points[1] || p0;
  const x0 = giToX(p0.gi, w), y0 = priceToY(p0.price, h);
  const x1 = giToX(p1.gi, w), y1 = priceToY(p1.price, h);
  if (y0 === null || y1 === null) return;

  if (d.type === 'cursor') return;

  if (d.type === 'line') {
    dc.setLineDash([]);
    dc.beginPath(); dc.moveTo(x0, y0); dc.lineTo(x1, y1); dc.stroke();
    // endpoints
    dc.beginPath(); dc.arc(x0, y0, 3*dpr, 0, Math.PI*2); dc.fill();
    dc.beginPath(); dc.arc(x1, y1, 3*dpr, 0, Math.PI*2); dc.fill();
  }

  if (d.type === 'ray') {
    // extend from p0 through p1 to edge
    dc.setLineDash([]);
    const dx = x1 - x0, dy = y1 - y0;
    const len = Math.sqrt(dx*dx + dy*dy) || 1;
    const ex = x0 + (dx/len) * Math.max(w, h) * 3;
    const ey = y0 + (dy/len) * Math.max(w, h) * 3;
    dc.beginPath(); dc.moveTo(x0, y0); dc.lineTo(ex, ey); dc.stroke();
    dc.beginPath(); dc.arc(x0, y0, 3*dpr, 0, Math.PI*2); dc.fill();
  }

  if (d.type === 'hline') {
    dc.setLineDash([6*dpr, 3*dpr]);
    dc.beginPath(); dc.moveTo(axisL, y0); dc.lineTo(w - 10, y0); dc.stroke();
    dc.setLineDash([]);
    // price label
    dc.font = `${Math.round(11*dpr)}px sans-serif`;
    dc.textAlign = 'right';
    dc.textBaseline = 'middle';
    dc.fillStyle = d.color || '#3b82f6';
    dc.fillText(fmtAxisNum(p0.price), axisL - 4, y0);
    dc.textAlign = 'start'; dc.textBaseline = 'alphabetic';
  }

  if (d.type === 'channel') {
    const dy = y1 - y0;
    const dx2 = x1 - x0, dy2 = dy;
    dc.setLineDash([]);
    dc.beginPath(); dc.moveTo(x0, y0); dc.lineTo(x1, y1); dc.stroke();
    // parallel offset: perpendicular distance stored in p0.channelOffset (price diff)
    const off = (d.channelOffset || 0);
    const yo0 = priceToY(p0.price + off, h);
    const yo1 = priceToY(p1.price + off, h);
    if (yo0 !== null && yo1 !== null) {
      dc.setLineDash([6*dpr, 3*dpr]);
      dc.beginPath(); dc.moveTo(x0, yo0); dc.lineTo(x1, yo1); dc.stroke();
      dc.setLineDash([]);
    }
  }

  if (d.type === 'fib' || d.type === 'fibext') {
    const levels = d.type === 'fib' ? FIB_LEVELS : FIB_EXT_LEVELS;
    const hiP = Math.max(p0.price, p1.price);
    const loP = Math.min(p0.price, p1.price);
    const range = hiP - loP;
    const xL = axisL, xR = w - 10;
    dc.font = `${Math.round(10*dpr)}px sans-serif`;
    dc.textAlign = 'left';
    dc.textBaseline = 'middle';
    for (let li = 0; li < levels.length; li++) {
      const lvl = levels[li];
      const price = d.type === 'fib' ? (hiP - lvl * range) : (loP + lvl * range);
      const yy = priceToY(price, h);
      if (yy === null) continue;
      const col = FIB_COLORS[li % FIB_COLORS.length];
      dc.strokeStyle = col; dc.fillStyle = col; dc.globalAlpha = 0.85;
      dc.setLineDash([4*dpr, 3*dpr]);
      dc.beginPath(); dc.moveTo(xL, yy); dc.lineTo(xR, yy); dc.stroke();
      dc.setLineDash([]); dc.globalAlpha = 1.0;
      dc.fillText(`${lvl.toFixed(3)}  ${fmtAxisNum(price)}`, xL + 4, yy - 8*dpr);
    }
    dc.textAlign = 'start'; dc.textBaseline = 'alphabetic';
    dc.strokeStyle = d.color || '#3b82f6'; dc.setLineDash([]);
    dc.beginPath(); dc.moveTo(x0, y0); dc.lineTo(x1, y1); dc.stroke();
    dc.beginPath(); dc.arc(x0, y0, 3*dpr, 0, Math.PI*2); dc.fill();
    dc.beginPath(); dc.arc(x1, y1, 3*dpr, 0, Math.PI*2); dc.fill();
  }

  if (d.type === 'highlow') {
    const s0 = (viewState.row && viewState.row.series) ? viewState.row.series : {};
    const his = s0.high || [], los = s0.low || [];
    const st2 = viewState.start, end2 = viewState.end;
    let hiP2 = -Infinity, loP2 = Infinity;
    for (let i = st2; i <= end2; i++) {
      const hv = Number(his[i]), lv = Number(los[i]);
      if (Number.isFinite(hv)) hiP2 = Math.max(hiP2, hv);
      if (Number.isFinite(lv)) loP2 = Math.min(loP2, lv);
    }
    if (!Number.isFinite(hiP2) || !Number.isFinite(loP2)) { dc.globalAlpha = 1.0; return; }
    const range2 = hiP2 - loP2;
    const xL2 = axisL, xR2 = w - 10;
    dc.font = `${Math.round(10*dpr)}px sans-serif`;
    dc.textAlign = 'left'; dc.textBaseline = 'middle';
    const yHi2 = priceToY(hiP2, h), yLo2 = priceToY(loP2, h);
    if (yHi2 !== null) {
      dc.strokeStyle = '#ef4444'; dc.fillStyle = '#ef4444'; dc.globalAlpha = 1.0; dc.setLineDash([]);
      dc.beginPath(); dc.moveTo(xL2, yHi2); dc.lineTo(xR2, yHi2); dc.stroke();
      dc.fillText(`最高 ${fmtAxisNum(hiP2)}`, xL2 + 4, yHi2 - 8*dpr);
    }
    if (yLo2 !== null) {
      dc.strokeStyle = '#10b981'; dc.fillStyle = '#10b981'; dc.globalAlpha = 1.0; dc.setLineDash([]);
      dc.beginPath(); dc.moveTo(xL2, yLo2); dc.lineTo(xR2, yLo2); dc.stroke();
      dc.fillText(`最低 ${fmtAxisNum(loP2)}`, xL2 + 4, yLo2 + 4*dpr);
    }
    for (let li2 = 0; li2 < FIB_LEVELS.length; li2++) {
      const lvl2 = FIB_LEVELS[li2];
      const price2 = hiP2 - lvl2 * range2;
      const yy2 = priceToY(price2, h);
      if (yy2 === null) continue;
      const col2 = FIB_COLORS[li2 % FIB_COLORS.length];
      dc.strokeStyle = col2; dc.fillStyle = col2; dc.globalAlpha = 0.7;
      dc.setLineDash([4*dpr, 3*dpr]);
      dc.beginPath(); dc.moveTo(xL2, yy2); dc.lineTo(xR2, yy2); dc.stroke();
      dc.setLineDash([]); dc.globalAlpha = 0.9;
      dc.fillText(`${lvl2.toFixed(3)}`, xR2 - 36*dpr, yy2 - 6*dpr);
    }
    dc.textAlign = 'start'; dc.textBaseline = 'alphabetic'; dc.globalAlpha = 1.0;
  }

  if (d.type === 'text' && d.text) {
    dc.font = `bold ${Math.round(13*dpr)}px sans-serif`;
    dc.fillStyle = d.color || '#3b82f6'; dc.globalAlpha = 1.0; dc.textBaseline = 'bottom';
    dc.fillText(d.text, x0, y0); dc.textBaseline = 'alphabetic';
  }

  if (d.type === 'rect') {
    const rx0 = Math.min(x0, x1), ry0 = Math.min(y0, y1);
    const rw = Math.abs(x1 - x0), rh = Math.abs(y1 - y0);
    if (rw < 2 || rh < 2) { dc.globalAlpha = 1.0; return; }
    const col = d.color || '#3b82f6';
    // 半透明填充
    dc.globalAlpha = isPreview ? 0.10 : 0.13;
    dc.fillStyle = col;
    dc.beginPath();
    dc.roundRect ? dc.roundRect(rx0, ry0, rw, rh, 4*dpr) : dc.rect(rx0, ry0, rw, rh);
    dc.fill();
    // 边框
    dc.globalAlpha = isPreview ? 0.6 : 0.9;
    dc.strokeStyle = col;
    dc.setLineDash([]);
    dc.lineWidth = 1.5 * dpr;
    dc.beginPath();
    dc.roundRect ? dc.roundRect(rx0, ry0, rw, rh, 4*dpr) : dc.rect(rx0, ry0, rw, rh);
    dc.stroke();
    // 四个角的小方块
    const cs = 4 * dpr;
    dc.fillStyle = col; dc.globalAlpha = 1.0;
    [[rx0, ry0],[rx0+rw, ry0],[rx0, ry0+rh],[rx0+rw, ry0+rh]].forEach(([cx, cy]) => {
      dc.fillRect(cx - cs/2, cy - cs/2, cs, cs);
    });
    // 价格标注（左侧顶部和底部）
    const priceTop = Math.max(p0.price, p1.price);
    const priceBtm = Math.min(p0.price, p1.price);
    const priceDiff = priceTop - priceBtm;
    const pricePct = (Number.isFinite(p0.price) && p0.price !== 0)
      ? ((p1.price - p0.price) / Math.abs(p0.price) * 100) : null;
    // K线根数和时间跨度
    const gi0 = Math.min(p0.gi, p1.gi), gi1 = Math.max(p0.gi, p1.gi);
    const kCount = gi1 - gi0 + 1;
    const bh2 = Number(viewState.barHours || 1);
    const totalH2 = kCount * bh2;
    const days2 = Math.floor(totalH2 / 24);
    const remH2 = Math.round(totalH2 % 24);
    const timeStr = days2 > 0 ? `${days2}天${remH2}小时` : `${remH2}小时`;
    dc.font = `${Math.round(10*dpr)}px sans-serif`;
    dc.fillStyle = col; dc.textAlign = 'right'; dc.textBaseline = 'middle';
    dc.fillText(fmtAxisNum(priceTop), rx0 - 4, Math.min(y0, y1));
    dc.fillText(fmtAxisNum(priceBtm), rx0 - 4, Math.max(y0, y1));
    // 框内中央显示统计信息（仿TradingView）
    if (rh > 28 * dpr && rw > 60 * dpr) {
      const sign = (pricePct || 0) >= 0 ? '+' : '';
      const pctStr = pricePct !== null ? `${sign}${pricePct.toFixed(2)}%` : '';
      const diffStr = fmtAxisNum(priceDiff);
      const line1 = pctStr ? `${diffStr}  (${pctStr})` : diffStr;
      const line2 = `${kCount}根K线，${timeStr}`;
      dc.textAlign = 'center'; dc.textBaseline = 'middle';
      dc.globalAlpha = 0.95;
      dc.font = `bold ${Math.round(11*dpr)}px sans-serif`;
      dc.fillText(line1, rx0 + rw/2, ry0 + rh/2 - 8*dpr);
      dc.font = `${Math.round(10*dpr)}px sans-serif`;
      dc.globalAlpha = 0.75;
      dc.fillText(line2, rx0 + rw/2, ry0 + rh/2 + 8*dpr);
    }
    dc.textAlign = 'start'; dc.textBaseline = 'alphabetic';
    dc.globalAlpha = 1.0;
  }

  if (d.type === 'wave' && d.points && d.points.length >= 2) {
    // 波浪理论标注：连线 + 圆圈数字标注
    const labels = d.labelSet === 1 ? WAVE_LABELS_CORRECTIVE : WAVE_LABELS_IMPULSE;
    const pts = d.points;
    const col = d.color || '#f59e0b';
    dc.strokeStyle = col;
    dc.fillStyle = col;
    dc.setLineDash([4*dpr, 2*dpr]);
    dc.lineWidth = 1.8 * dpr;
    // 连接各波浪点
    dc.beginPath();
    for (let wi = 0; wi < pts.length; wi++) {
      const px = giToX(pts[wi].gi, w);
      const py = priceToY(pts[wi].price, h);
      if (py === null) continue;
      if (wi === 0) dc.moveTo(px, py); else dc.lineTo(px, py);
    }
    dc.stroke();
    dc.setLineDash([]);
    // 绘制标注点和标签
    dc.font = `bold ${Math.round(12*dpr)}px 'Georgia', serif`;
    dc.textAlign = 'center';
    dc.textBaseline = 'middle';
    for (let wi = 0; wi < pts.length; wi++) {
      const px = giToX(pts[wi].gi, w);
      const py = priceToY(pts[wi].price, h);
      if (py === null) continue;
      const label = labels[wi] || String(wi + 1);
      const r = 9 * dpr;
      // 判断上方还是下方放标签（奇数点在上，偶数在下，从1开始）
      const above = (wi % 2 === 0);
      const labelY = above ? py - r - 6*dpr : py + r + 6*dpr;
      // 圆圈背景
      dc.globalAlpha = 0.92;
      dc.fillStyle = col;
      dc.beginPath(); dc.arc(px, labelY, r, 0, Math.PI * 2); dc.fill();
      // 标签文字
      dc.globalAlpha = 1.0;
      dc.fillStyle = '#ffffff';
      dc.fillText(label, px, labelY);
      // 小圆点在价格位
      dc.fillStyle = col;
      dc.beginPath(); dc.arc(px, py, 3*dpr, 0, Math.PI*2); dc.fill();
    }
    dc.textAlign = 'start'; dc.textBaseline = 'alphabetic'; dc.globalAlpha = 1.0;
  }

  dc.globalAlpha = 1.0;
}

// ---- 指标信息框 ----
function updateInfoPanel() {
  const panel = document.getElementById('tvInfoPanel');
  const content = document.getElementById('tvInfoContent');
  if (!panel || !content) return;
  if (!drawState.infoVisible) { panel.classList.remove('visible'); return; }
  panel.classList.add('visible');
  const row = viewState.row;
  if (!row) { content.innerHTML = '<div class="tv-info-label">无数据</div>'; return; }
  const s = row.series || {};
  const closes = s.close || [];
  const hi = viewState.hoverLocalIdx;
  const gi = (Number.isFinite(Number(hi)) ? viewState.start + Math.trunc(Number(hi)) : viewState.end);
  const c = Number(closes[gi]);
  const o = Number((s.open || [])[gi]);
  const hh = Number((s.high || [])[gi]);
  const ll = Number((s.low || [])[gi]);
  const vol = Number((s.volume || [])[gi]);
  const pct = Number.isFinite(o) && o !== 0 ? ((c/o-1)*100) : null;
  const ms = barTimeMs(gi);
  const pctClass = pct === null ? '' : (pct >= 0 ? 'up' : 'down');
  const pctText = pct === null ? '-' : (pct >= 0 ? '+' : '') + pct.toFixed(2) + '%';
  const rangeN = viewState.n;
  const bh = Number(viewState.barHours || 1);
  const totalH = rangeN * bh;
  const days = Math.floor(totalH / 24);
  const remH = Math.round(totalH % 24);
  const rangeStr = days > 0 ? `${days}天${remH}小时` : `${remH}小时`;
  const st2 = viewState.start, end2 = viewState.end;
  const firstClose = Number(closes[st2]), lastClose = Number(closes[end2]);
  const rangePct = (Number.isFinite(firstClose) && firstClose !== 0 && Number.isFinite(lastClose))
    ? ((lastClose/firstClose - 1)*100) : null;
  const rangePctClass = rangePct === null ? '' : (rangePct >= 0 ? 'up' : 'down');
  const rangePctStr = rangePct === null ? '-' : (rangePct >= 0 ? '+' : '') + rangePct.toFixed(2) + '%';
  const rows = [
    ['时间', fmtTs(ms)],
    ['开盘', fmtNum(o)],
    ['最高', fmtNum(hh)],
    ['最低', fmtNum(ll)],
    ['收盘', `<span class="tv-info-value ${pctClass}">${fmtNum(c)}</span>`],
    ['涨跌', `<span class="tv-info-value ${pctClass}">${pctText}</span>`],
    ['成交量', fmtAxisNum(vol)],
    ['区间', `<span class="tv-info-badge">${rangeN}根·${rangeStr}</span>`],
    ['区间涨跌', `<span class="tv-info-value ${rangePctClass}">${rangePctStr}</span>`],
  ];
  for (const ov of (viewState.overlaysAll || [])) {
    if (isKeyHidden(ov.key)) continue;
    const v = Number((ov.series || [])[gi]);
    if (Number.isFinite(v)) rows.push([ov.name || ov.key, fmtNum(v)]);
  }
  content.innerHTML = rows.map(([k, v]) =>
    `<div class="tv-info-row"><span class="tv-info-label">${k}</span><span class="tv-info-value">${v}</span></div>`
  ).join('');
}

// ---- 统计面板（右上角仿TradingView数据窗口） ----
function updateStatsPanel() {
  const panel = document.getElementById('tvStatsPanel');
  const body = document.getElementById('tvStatsBody');
  const symEl = document.getElementById('tvStatsSymbol');
  const tfEl = document.getElementById('tvStatsTf');
  if (!panel) return;
  if (!drawState.statsVisible || !viewState.row) {
    panel.classList.remove('visible');
    return;
  }
  panel.classList.add('visible');
  const row = viewState.row;
  const s = row.series || {};
  const closes = s.close || [];
  const opens = s.open || [];
  const highs = s.high || [];
  const lows = s.low || [];
  const volumes = s.volume || [];
  const hi = viewState.hoverLocalIdx;
  const gi = (Number.isFinite(Number(hi)) ? viewState.start + Math.trunc(Number(hi)) : viewState.end);
  const c = Number(closes[gi]);
  const o = Number(opens[gi]);
  const hh = Number(highs[gi]);
  const ll = Number(lows[gi]);
  const vol = Number(volumes[gi]);
  const pct = Number.isFinite(o) && o !== 0 ? ((c/o-1)*100) : null;
  const ms = barTimeMs(gi);
  // range stats
  const st2 = viewState.start, end2 = viewState.end;
  const n = viewState.n;
  const bh2 = Number(viewState.barHours || 1);
  const totalH = n * bh2;
  const days = Math.floor(totalH / 24);
  const remH = Math.round(totalH % 24);
  const rangeStr = days > 0 ? `${days}天${remH}小时` : `${remH}小时`;
  const firstClose = Number(closes[st2]);
  const lastClose = Number(closes[end2]);
  const rangePct = (Number.isFinite(firstClose) && firstClose !== 0 && Number.isFinite(lastClose))
    ? ((lastClose/firstClose - 1)*100) : null;
  // symbol & tf
  if (symEl) symEl.textContent = stripQuote(row.symbol || '-');
  if (tfEl) tfEl.textContent = bh2 === 1 ? '1H' : bh2 === 4 ? '4H' : bh2 === 24 ? '1D' : `${bh2}H`;
  if (!body) return;
  const pctClass = pct === null ? '' : (pct >= 0 ? 'up' : 'down');
  const pctStr = pct === null ? '-' : (pct >= 0 ? '+' : '') + pct.toFixed(2) + '%';
  const rangePctClass = rangePct === null ? '' : (rangePct >= 0 ? 'up' : 'down');
  const rangePctStr = rangePct === null ? '-' : (rangePct >= 0 ? '+' : '') + rangePct.toFixed(2) + '%';
  const rows = [
    ['时间', fmtTs(ms)],
    ['开盘', fmtNum(o)],
    ['最高', fmtNum(hh)],
    ['最低', fmtNum(ll)],
    ['收盘', `<span class="tv-stats-value ${pctClass}">${fmtNum(c)}</span>`],
    ['涨跌', `<span class="tv-stats-value ${pctClass}">${pctStr}</span>`],
    ['成交量', fmtAxisNum(vol)],
  ];
  for (const ov of (viewState.overlaysAll || [])) {
    if (isKeyHidden(ov.key)) continue;
    const v = Number((ov.series || [])[gi]);
    if (Number.isFinite(v)) rows.push([ov.name || ov.key, fmtNum(v)]);
  }
  const rowsHtml = rows.map(([k, v]) =>
    `<div class="tv-stats-row"><span class="tv-stats-label">${k}</span><span class="tv-stats-value">${v}</span></div>`
  ).join('');
  body.innerHTML = rowsHtml +
    `<div class="tv-stats-divider"></div>` +
    `<div class="tv-stats-range-chip"><span class="tv-stats-value ${rangePctClass}" style="font-size:11px">${rangePctStr}</span><span style="color:var(--text-dim);font-size:10px">&nbsp;${n}根·${rangeStr}</span></div>`;
}

// ---- 颜色选择器 ----
function initColorPicker() {
  const cp = document.getElementById('tvColorPicker');
  const sw = document.getElementById('tvColorSwatches');
  if (!cp || !sw) return;
  sw.innerHTML = '';
  for (const col of DRAW_COLORS) {
    const btn = document.createElement('button');
    btn.className = 'tv-cp-swatch';
    btn.style.background = col;
    btn.title = col;
    if (col === drawState.color) btn.classList.add('selected');
    btn.addEventListener('click', () => {
      drawState.color = col;
      sw.querySelectorAll('.tv-cp-swatch').forEach(b => b.classList.remove('selected'));
      btn.classList.add('selected');
      cp.style.display = 'none';
    });
    sw.appendChild(btn);
  }
}

// ---- 工具栏初始化 ----
function initToolbar() {
  const toolbar = document.getElementById('tvToolbar');
  if (!toolbar) return;
  function setTool(tool) {
    drawState.tool = tool;
    drawState.drawing = false;
    drawState.tempStart = null;
    drawState.tempEnd = null;
    toolbar.querySelectorAll('.tv-tool-btn[data-tool]').forEach(b => {
      b.classList.toggle('active', b.dataset.tool === tool);
    });
    const dc = drawState.drawCanvas;
    if (dc) {
      dc.classList.remove('active','line-active','text-active','erase-active');
      if (tool !== 'cursor' && tool !== 'info' && tool !== 'highlow') {
        dc.style.pointerEvents = 'auto';
        dc.classList.add('active', tool === 'text' ? 'text-active' : tool === 'erase' ? 'erase-active' : 'line-active');
      } else {
        dc.style.pointerEvents = 'none';
      }
    }
    if (tool !== 'info') { drawState.infoVisible = false; updateInfoPanel(); }
    redrawDrawings();
  }
  toolbar.querySelectorAll('.tv-tool-btn[data-tool]').forEach(btn => {
    btn.addEventListener('click', () => {
      const tool = btn.dataset.tool;
      if (tool === 'info') {
        drawState.infoVisible = !drawState.infoVisible;
        btn.classList.toggle('active', drawState.infoVisible);
        updateInfoPanel();
        return;
      }
      if (tool === 'highlow') {
        drawState.drawings.push({ type: 'highlow', points: [{ gi: viewState.start, price: 0 }, { gi: viewState.end, price: 0 }], color: drawState.color });
        redrawDrawings();
        return;
      }
      if (tool === 'wave') {
        // 切换标签集（右键切换，左键选工具）
        setTool(tool);
        drawState.wavePoints = [];
        return;
      }
      setTool(tool);
    });
  });
  const clearBtn = document.getElementById('toolClear');
  if (clearBtn) {
    clearBtn.addEventListener('click', () => {
      if (!drawState.drawings.length) return;
      if (!confirm('清除所有绘图？')) return;
      drawState.drawings = [];
      redrawDrawings();
    });
  }
  toolbar.addEventListener('contextmenu', (e) => {
    const btn = e.target.closest('.tv-tool-btn[data-tool]');
    if (!btn) return;
    e.preventDefault();
    if (btn.dataset.tool === 'wave') {
      // 右键切换波浪标签集
      drawState.waveLabelSet = drawState.waveLabelSet === 0 ? 1 : 0;
      drawState.wavePoints = [];
      const labels = drawState.waveLabelSet === 1 ? 'A-B-C调整浪' : '①-⑤推进浪';
      showAlert(`波浪标注切换为：${labels}（右键再次切换）`);
      setTimeout(() => showAlert(''), 2500);
      return;
    }
    if (btn.dataset.tool === 'cursor') return;
    const cp = document.getElementById('tvColorPicker');
    if (!cp) return;
    cp.style.display = 'block';
    cp.style.left = (e.clientX + 10) + 'px';
    cp.style.top = Math.min(e.clientY, window.innerHeight - 220) + 'px';
  });
  // 波浪工具：按 Escape 取消当前波浪
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && drawState.tool === 'wave' && drawState.wavePoints.length > 0) {
      drawState.wavePoints = [];
      redrawDrawings();
    }
  });
  document.addEventListener('click', (e) => {
    const cp = document.getElementById('tvColorPicker');
    if (cp && !cp.contains(e.target)) cp.style.display = 'none';
  });
  initColorPicker();
  initStatsPanelDrag();
  initInfoPanelDrag();
}

// ---- 统计面板拖拽移动 + 调整大小 ----
function initStatsPanelDrag() {
  const panel = document.getElementById('tvStatsPanel');
  if (!panel) return;

  // 添加调整大小的把手
  const resizer = document.createElement('div');
  resizer.className = 'tv-stats-resizer';
  resizer.title = '拖拽调整大小';
  panel.appendChild(resizer);

  // 拖拽移动
  let dragX = 0, dragY = 0, startL = 0, startT = 0, isDragging = false;
  const head = panel.querySelector('.tv-stats-head');
  if (head) {
    head.style.cursor = 'move';
    head.addEventListener('pointerdown', (e) => {
      if (e.target === resizer) return;
      isDragging = true;
      dragX = e.clientX; dragY = e.clientY;
      const r = panel.getBoundingClientRect();
      startL = r.left; startT = r.top;
      panel.style.right = 'auto';
      panel.style.left = startL + 'px';
      panel.style.top = startT + 'px';
      head.setPointerCapture(e.pointerId);
      e.preventDefault();
    });
    head.addEventListener('pointermove', (e) => {
      if (!isDragging) return;
      const dx = e.clientX - dragX, dy = e.clientY - dragY;
      const newL = Math.max(0, Math.min(window.innerWidth - panel.offsetWidth, startL + dx));
      const newT = Math.max(0, Math.min(window.innerHeight - panel.offsetHeight, startT + dy));
      panel.style.left = newL + 'px';
      panel.style.top = newT + 'px';
    });
    head.addEventListener('pointerup', () => { isDragging = false; });
    head.addEventListener('pointercancel', () => { isDragging = false; });
  }

  // 调整大小
  let resizing = false, rsX = 0, rsY = 0, rsW = 0, rsH = 0;
  resizer.addEventListener('pointerdown', (e) => {
    resizing = true;
    rsX = e.clientX; rsY = e.clientY;
    rsW = panel.offsetWidth; rsH = panel.offsetHeight;
    resizer.setPointerCapture(e.pointerId);
    e.preventDefault(); e.stopPropagation();
  });
  resizer.addEventListener('pointermove', (e) => {
    if (!resizing) return;
    const newW = Math.max(160, rsW + (e.clientX - rsX));
    const newH = Math.max(120, rsH + (e.clientY - rsY));
    panel.style.width = newW + 'px';
    panel.style.maxHeight = newH + 'px';
    panel.style.overflowY = 'auto';
  });
  resizer.addEventListener('pointerup', () => { resizing = false; });
  resizer.addEventListener('pointercancel', () => { resizing = false; });
}

// ---- 指标信息浮框拖拽 ----
function initInfoPanelDrag() {
  const panel = document.getElementById('tvInfoPanel');
  const head = document.getElementById('tvInfoPanelHead');
  if (!panel || !head) return;
  let isDragging = false, dragX = 0, dragY = 0, startL = 0, startT = 0;
  head.addEventListener('pointerdown', (e) => {
    isDragging = true;
    dragX = e.clientX; dragY = e.clientY;
    const r = panel.getBoundingClientRect();
    startL = r.left; startT = r.top;
    panel.style.left = startL + 'px';
    panel.style.top = startT + 'px';
    head.setPointerCapture(e.pointerId);
    e.preventDefault();
  });
  head.addEventListener('pointermove', (e) => {
    if (!isDragging) return;
    const dx = e.clientX - dragX, dy = e.clientY - dragY;
    const newL = Math.max(0, Math.min(window.innerWidth - panel.offsetWidth, startL + dx));
    const newT = Math.max(0, Math.min(window.innerHeight - panel.offsetHeight, startT + dy));
    panel.style.left = newL + 'px';
    panel.style.top = newT + 'px';
  });
  head.addEventListener('pointerup', () => { isDragging = false; });
  head.addEventListener('pointercancel', () => { isDragging = false; });
}
function attachDrawCanvas() {
  const candlePanel = document.querySelector('.kline-candle-panel');
  if (!candlePanel || candlePanel.querySelector('.kline-draw-layer')) return;
  const dc = document.createElement('canvas');
  dc.className = 'kline-draw-layer';
  dc.style.position = 'absolute';
  dc.style.inset = '0';
  dc.style.zIndex = '5';
  dc.style.borderRadius = '10px';
  dc.style.pointerEvents = 'none';
  candlePanel.appendChild(dc);
  drawState.drawCanvas = dc;

  function ensureDraw() {
    const ref = candlePanel.querySelector('canvas.kline-canvas');
    if (!ref) return;
    if (dc.width !== ref.width || dc.height !== ref.height) {
      dc.width = ref.width; dc.height = ref.height;
      dc.style.width = (ref.offsetWidth || ref.clientWidth) + 'px';
      dc.style.height = (ref.offsetHeight || ref.clientHeight) + 'px';
    }
    drawState.drawCtx = dc.getContext('2d');
  }

  if (window.ResizeObserver) {
    new ResizeObserver(() => { ensureDraw(); redrawDrawings(); }).observe(candlePanel);
  }

  dc.addEventListener('pointerdown', (e) => {
    if (drawState.tool === 'cursor' || drawState.tool === 'info') return;
    ensureDraw();
    const pt = canvasToPlot(dc, e.clientX, e.clientY);
    if (drawState.tool === 'text') {
      const inp = document.createElement('input');
      inp.className = 'tv-text-input';
      inp.style.left = e.clientX + 'px';
      inp.style.top = (e.clientY - 28) + 'px';
      inp.style.position = 'fixed';
      inp.placeholder = '输入文字…';
      document.body.appendChild(inp);
      inp.focus();
      const commit = () => {
        const txt = inp.value.trim();
        if (txt) drawState.drawings.push({ type: 'text', points: [pt], color: drawState.color, text: txt });
        inp.remove(); redrawDrawings();
      };
      inp.addEventListener('keydown', (ke) => { if (ke.key === 'Enter' || ke.key === 'Escape') commit(); });
      inp.addEventListener('blur', commit);
      return;
    }
    if (drawState.tool === 'erase') {
      // 橡皮擦画笔：按下即开始，划过的绘图都删除
      drawState.erasing = true;
      dc.setPointerCapture(e.pointerId);
      return;
    }
    if (drawState.tool === 'wave') {
      // 逐点模式：每次点击添加一个波浪节点
      const labels = drawState.waveLabelSet === 1 ? WAVE_LABELS_CORRECTIVE : WAVE_LABELS_IMPULSE;
      drawState.wavePoints.push(pt);
      // 达到最大标注数时自动提交
      if (drawState.wavePoints.length >= labels.length) {
        drawState.drawings.push({
          type: 'wave',
          points: [...drawState.wavePoints],
          color: drawState.color,
          labelSet: drawState.waveLabelSet,
        });
        drawState.wavePoints = [];
        redrawDrawings();
      } else {
        // 预览已点的节点
        redrawDrawings();
      }
      return;
    }
    e.preventDefault();
    drawState.drawing = true;
    drawState.tempStart = pt;
    drawState.tempEnd = pt;
    try { dc.setPointerCapture(e.pointerId); } catch {}
  });

  dc.addEventListener('pointermove', (e) => {
    const pt = canvasToPlot(dc, e.clientX, e.clientY);
    if (drawState.tool === 'erase') {
      ensureDraw();
      redrawDrawings();
      const dpr = window.devicePixelRatio || 1;
      const ctx2 = drawState.drawCtx;
      if (ctx2) {
        const r = 14 * dpr;
        ctx2.save();
        ctx2.strokeStyle = 'rgba(239,68,68,0.85)';
        ctx2.lineWidth = 1.5 * dpr;
        ctx2.setLineDash([]);
        ctx2.beginPath(); ctx2.arc(pt.px, pt.py, r, 0, Math.PI * 2); ctx2.stroke();
        const hs = 5 * dpr;
        ctx2.beginPath();
        ctx2.moveTo(pt.px - hs, pt.py); ctx2.lineTo(pt.px + hs, pt.py);
        ctx2.moveTo(pt.px, pt.py - hs); ctx2.lineTo(pt.px, pt.py + hs);
        ctx2.stroke();
        ctx2.restore();
      }
      // 无论是否按下，只要移动就检测并删除碰到的绘图
      const eraseR = 20 * dpr;
      const toRemove = new Set();
      for (let i = 0; i < drawState.drawings.length; i++) {
        const dr = drawState.drawings[i];
        if (!dr.points || dr.points.length < 1) continue;
        let hit = false;
        // 检测控制点距离
        for (const pp of dr.points) {
          const px = giToX(pp.gi, dc.width);
          const py = priceToY(pp.price, dc.height);
          if (py === null) continue;
          if (Math.hypot(pt.px - px, pt.py - py) <= eraseR) { hit = true; break; }
        }
        // 检测线段中间（趋势线、射线、通道等）
        if (!hit && dr.points.length >= 2) {
          const ax = giToX(dr.points[0].gi, dc.width);
          const ay = priceToY(dr.points[0].price, dc.height);
          const bx = giToX(dr.points[1].gi, dc.width);
          const by = priceToY(dr.points[1].price, dc.height);
          if (ay !== null && by !== null) {
            const ddx = bx - ax, ddy = by - ay;
            const len2 = ddx*ddx + ddy*ddy;
            if (len2 > 0) {
              const tt = Math.max(0, Math.min(1, ((pt.px-ax)*ddx + (pt.py-ay)*ddy) / len2));
              const cx2 = ax + tt*ddx, cy2 = ay + tt*ddy;
              if (Math.hypot(pt.px - cx2, pt.py - cy2) <= eraseR) hit = true;
            }
          }
        }
        // 水平线
        if (!hit && dr.type === 'hline') {
          const py = priceToY(dr.points[0].price, dc.height);
          if (py !== null && Math.abs(pt.py - py) <= eraseR) hit = true;
        }
        // 矩形
        if (!hit && dr.type === 'rect' && dr.points[1]) {
          const p0r = dr.points[0], p1r = dr.points[1];
          const rx0 = Math.min(giToX(p0r.gi, dc.width), giToX(p1r.gi, dc.width));
          const rx1 = Math.max(giToX(p0r.gi, dc.width), giToX(p1r.gi, dc.width));
          const ry0v = priceToY(p0r.price, dc.height);
          const ry1v = priceToY(p1r.price, dc.height);
          if (ry0v !== null && ry1v !== null) {
            const ry0 = Math.min(ry0v, ry1v), ry1 = Math.max(ry0v, ry1v);
            if (pt.px >= rx0 && pt.px <= rx1 && pt.py >= ry0 && pt.py <= ry1) hit = true;
          }
        }
        if (hit) toRemove.add(i);
      }
      if (toRemove.size > 0) {
        drawState.drawings = drawState.drawings.filter((_, i) => !toRemove.has(i));
        redrawDrawings();
      }
      return;
    }
    if (!drawState.drawing) return;
    ensureDraw();
    drawState.tempEnd = pt;
    redrawDrawings();
  });

  dc.addEventListener('pointerup', (e) => {
    if (drawState.erasing) { drawState.erasing = false; return; }
    if (!drawState.drawing) return;
    drawState.drawing = false;
    const pt1 = canvasToPlot(dc, e.clientX, e.clientY);
    const p0 = drawState.tempStart;
    if (p0) {
      const tool = drawState.tool;
      const d = { type: tool, points: [p0, pt1], color: drawState.color };
      if (tool === 'channel') {
        d.channelOffset = Math.abs(p0.price - pt1.price) * 0.25;
      }
      drawState.drawings.push(d);
    }
    drawState.tempStart = null;
    drawState.tempEnd = null;
    redrawDrawings();
  });

  dc.addEventListener('pointercancel', () => {
    drawState.drawing = false;
    drawState.erasing = false;
    drawState.tempStart = null;
    drawState.tempEnd = null;
    redrawDrawings();
  });

  // 双击删除最近绘图
  const refCanvas = document.getElementById('candleCanvas');
  if (refCanvas) {
    refCanvas.addEventListener('dblclick', (e) => {
      if (drawState.tool !== 'cursor') return;
      ensureDraw();
      const pt = canvasToPlot(dc, e.clientX, e.clientY);
      let bestIdx = -1, bestDist = 60;
      for (let i = 0; i < drawState.drawings.length; i++) {
        const d = drawState.drawings[i];
        if (!d.points || !d.points[0]) continue;
        const px = giToX(d.points[0].gi, dc.width);
        const py = priceToY(d.points[0].price, dc.height);
        if (px === null || py === null) continue;
        const dpr = window.devicePixelRatio || 1;
        const dist = Math.hypot((pt.px - px)/dpr, (pt.py - py)/dpr);
        if (dist < bestDist) { bestDist = dist; bestIdx = i; }
      }
      if (bestIdx >= 0) { drawState.drawings.splice(bestIdx, 1); redrawDrawings(); }
    });
  }
}

// 绘图层同步已集成进 scheduleDraw 的 requestAnimationFrame 回调中，无需额外钩子

function initKlinePage() {
  wireGlobalErrorHandler();
  const back = $("btnBack");
  const reload = $("btnReload");
  const btnIndicators = $("btnIndicators");
  const btnCloseIndicators = $("btnCloseIndicators");
  const btnResetView = $("btnResetView");
  const tailSelect = $("tailSelect");
  if (back) {
    back.addEventListener("click", () => {
      if (window.history.length > 1) { window.history.back(); return; }
      window.location.href = "./index.html";
    });
  }
  if (reload) reload.addEventListener("click", () => render());
  if (tailSelect) {
    const v0 = loadTailSetting();
    tailSelect.value = String(v0);
    tailSelect.addEventListener("change", () => {
      const v = Math.trunc(Number(tailSelect.value));
      const next = [360,720,1440,2160,3650].includes(v) ? v : 360;
      saveTailSetting(next);
      showAlert(`已选择显示 ${next} 根K线；点击"重新加载"后生效。`);
    });
  }
  if (btnIndicators) btnIndicators.addEventListener("click", () => setIndicatorsOpen(true));
  if (btnCloseIndicators) btnCloseIndicators.addEventListener("click", () => setIndicatorsOpen(false));
  const settings = document.getElementById("indicatorSettings");
  if (settings) settings.addEventListener("click", (e) => { if (e && e.target === settings) setIndicatorsOpen(false); });
  if (btnResetView) {
    btnResetView.addEventListener("click", () => {
      viewState.viewN = loadTailSetting();
      viewState.offset = 0;
      viewState.hoverLocalIdx = null;
      showTooltip("", 0, 0);
      scheduleDraw();
    });
  }
  attachInteractions($("candleCanvas"));
  try {
    if (window.ResizeObserver) {
      const ro = new ResizeObserver(() => { if (viewState.row) scheduleDraw(); });
      const candlePanel = document.querySelector(".kline-candle-panel");
      const indPanel = document.getElementById("indicatorPanel");
      if (candlePanel) ro.observe(candlePanel);
      if (indPanel) ro.observe(indPanel);
    }
  } catch {}
  window.addEventListener("resize", () => { if (viewState.row) scheduleDraw(); else render(); });
  window.addEventListener("storage", (e) => {
    if (e && e.key === themeStorageKey) {
      applyThemeFromStorage();
      if (viewState.row) scheduleDraw(); else render();
    }
  });
  applyThemeFromStorage();
  initToolbar();
  attachDrawCanvas();
  requestAnimationFrame(() => render());
  setTimeout(() => render(), 250);
}

initKlinePage();
})(); 
