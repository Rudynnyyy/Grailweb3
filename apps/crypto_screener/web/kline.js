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

function drawCandles(ctx, row, overlays, start, n, endIdx, labelAt, hoverLocalIdx) {
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
  const pw = w - L - R;
  const ph = h - T - B;
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

  drawCandles(candleCtx, viewState.row, overlays, start, n, end, labelAt, viewState.hoverLocalIdx);

  const panels = viewState.indicatorPanels || [];
  for (const p of panels) {
    const ctx = ensureCanvas(p.canvas);
    if (!ctx) continue;
    const groups = (p.items || [])
      .filter((x) => !isKeyHidden(x && x.key ? x.key : ""))
      .map((x) => ({ ...x, series: (x.series || []).slice(start, end + 1) }));
    drawIndicators(ctx, groups, n, labelAt, p.title);
  }
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
    el.textContent = "";
    return;
  }
  el.classList.remove("hidden");
  el.textContent = String(text);
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
  const pct = Number.isFinite(o) && o !== 0 && Number.isFinite(c) ? ((c / o - 1) * 100) : null;
  const lines = [];
  lines.push(fmtTs(ms));
  lines.push(`开盘价 ${fmtNum(o)}  最高价 ${fmtNum(h)}`);
  lines.push(`最低价 ${fmtNum(l)}  收盘价 ${fmtNum(c)}`);
  lines.push(`涨跌幅 ${pct === null ? "-" : pct.toFixed(2) + "%"}`);
  for (const it of viewState.overlaysAll || []) {
    if (isKeyHidden(it && it.key ? it.key : "")) continue;
    const v = Number((it && it.series ? it.series : [])[gi]);
    lines.push(`${String(it.name || it.key)} ${Number.isFinite(v) ? fmtNum(v) : "-"}`);
  }
  for (const x of getAllIndicatorItems()) {
    const it = x.item;
    const v = Number((it && it.series ? it.series : [])[gi]);
    const prefix = x.panel ? String(x.panel.title || `副图${x.panel.panelId}`) : "副图";
    lines.push(`${prefix}｜${String(it.name || it.key)} ${Number.isFinite(v) ? fmtNum(v) : "-"}`);
  }
  return lines.join("\n");
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
      showTooltip(buildMainHoverText(gi), e.clientX + 12, e.clientY + 12);
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

initKlinePage();
})();
