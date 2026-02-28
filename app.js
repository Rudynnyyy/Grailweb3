const $ = (id) => document.getElementById(id);

const state = {
  meta: null,
  latest: null,
  customFactors: [],
  exprEditorOpen: false,
  exprDraftParams: [],
  lastAutoUpdateKey: null,
  columnVisibility: {}, // { key: boolean }
};

const storageKey = "crypto_screener_custom_factors_v1";
const columnVisibilityKey = "crypto_screener_column_visibility_v1";

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

function setStatus(text) {
  const el = $("status");
  if (!el) return;
  el.textContent = text || "";
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

async function loadJson(url) {
  const u = `${url}?t=${Date.now()}`;
  const res = await fetch(u, { cache: "no-store" });
  if (!res.ok) throw new Error(`请求失败：${res.status}`);
  return await res.json();
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
    if (Number.isFinite(Number(arr[i]))) {
      firstIdx = i;
      break;
    }
  }
  if (firstIdx === -1) return out;
  let lastEma = Number(arr[firstIdx]);
  out[firstIdx] = lastEma;
  for (let i = firstIdx + 1; i < arr.length; i++) {
    const v = Number(arr[i]);
    if (Number.isFinite(v)) {
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
      if (!node.args || node.args.length < 3) return vScalar(null);
      const x = ensureSeries(node.args[0]);
      const y = ensureSeries(node.args[1]);
      const w = ensureScalar(node.args[2]);
      if (!x.length || !y.length || w === null) return vScalar(null);
      return vScalar(rollingCorr(x, y, Math.trunc(w)));
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
    fields.push({ key: "ema", name: `EMA(${params.emaPeriod})`, type: "num", get: (r) => ema(getSeries(r, "close"), params.emaPeriod) });
  }
  if ($("condBollUp") && $("condBollUp").checked) {
    fields.push({ key: "boll_up", name: "BOLLUP", type: "num", get: (r) => {
      const closes = getSeries(r, "close");
      const ma = sma(closes, params.bollPeriod);
      const std = rollingStd(closes, params.bollPeriod);
      return (ma !== null && std !== null) ? ma + params.bollStd * std : null;
    }});
  }
  if ($("condBollDown") && $("condBollDown").checked) {
    fields.push({ key: "boll_down", name: "BOLLDOWN", type: "num", get: (r) => {
      const closes = getSeries(r, "close");
      const ma = sma(closes, params.bollDownPeriod);
      const std = rollingStd(closes, params.bollDownPeriod);
      return (ma !== null && std !== null) ? ma - params.bollDownStd * std : null;
    }});
  }
  if ($("condSuper") && $("condSuper").checked) {
    fields.push({ key: "supertrend", name: "Supertrend", type: "num", get: (r) => supertrend(getSeries(r, "high"), getSeries(r, "low"), getSeries(r, "close"), params.superAtrPeriod, params.superMult) });
  }
  if ($("condKdj") && $("condKdj").checked) {
    fields.push({ key: "kdj_k", name: "K", type: "num", get: (r) => kdj(getSeries(r, "high"), getSeries(r, "low"), getSeries(r, "close"), params.kdjN, params.kdjM1, params.kdjM2).k });
    fields.push({ key: "kdj_d", name: "D", type: "num", get: (r) => kdj(getSeries(r, "high"), getSeries(r, "low"), getSeries(r, "close"), params.kdjN, params.kdjM1, params.kdjM2).d });
    fields.push({ key: "kdj_j", name: "J", type: "num", get: (r) => kdj(getSeries(r, "high"), getSeries(r, "low"), getSeries(r, "close"), params.kdjN, params.kdjM1, params.kdjM2).j });
  }
  if ($("condObv") && $("condObv").checked) {
    fields.push({ key: "obv", name: "OBV", type: "num", get: (r) => obvWithMa(getSeries(r, "close"), getSeries(r, "volume"), params.obvMaPeriod).obv });
    fields.push({ key: "obv_ma", name: "OBV_MA", type: "num", get: (r) => obvWithMa(getSeries(r, "close"), getSeries(r, "volume"), params.obvMaPeriod).ma });
  }
  if ($("condStochRsi") && $("condStochRsi").checked) {
    fields.push({ key: "stoch_rsi_k", name: "StochK", type: "num", get: (r) => stochRsi(getSeries(r, "close"), params.stochRsiP, params.stochRsiK, params.stochRsiSmK, params.stochRsiSmD).k });
    fields.push({ key: "stoch_rsi_d", name: "StochD", type: "num", get: (r) => stochRsi(getSeries(r, "close"), params.stochRsiP, params.stochRsiK, params.stochRsiSmK, params.stochRsiSmD).d });
  }

  for (const f of customFactors.filter((x) => x.show)) {
    fields.push({ key: `expr_${f.id}`, name: f.name || f.expr, type: "num", get: (r) => (r._expr ? r._expr[f.id] : null) });
  }

  // Filter based on user visibility preference
  const finalFields = fields.filter(f => {
    if (state.columnVisibility[f.key] === undefined) return true; // Default visible
    return state.columnVisibility[f.key];
  });

  updateColumnSelector(fields);

  return finalFields;
}

function updateColumnSelector(allFields) {
  const host = $("columnSelector");
  if (!host) return;
  
  // Only rebuild if fields changed or first time
  const currentCount = host.querySelectorAll('label').length;
  if (currentCount === allFields.length) return;

  host.innerHTML = "";
  for (const f of allFields) {
    const label = document.createElement("label");
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = state.columnVisibility[f.key] !== false;
    cb.addEventListener("change", () => {
      state.columnVisibility[f.key] = cb.checked;
      saveColumnVisibility();
      refresh();
    });
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

  const nextKey = existed.has(prevKey) ? prevKey : fields.find((f) => f.key.startsWith("rsi_"))?.key || "close";
  sortKeyEl.value = nextKey;
  sortOrderEl.value = sortOrderEl.value || current?.order || "desc";
}

function buildTableHeader(fields) {
  const thead = $("thead");
  thead.innerHTML = "";
  const tr = document.createElement("tr");
  for (const f of fields) {
    const th = document.createElement("th");
    th.textContent = f.name || f.key;
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
    for (const f of fields) {
      const td = document.createElement("td");
      const raw = f.get ? f.get(r) : r[f.key];
      if (f.type === "dt") td.textContent = fmtDt(raw);
      else if (f.type === "num") {
        td.className = "num";
        td.textContent = fmtNum(raw);
      } else td.textContent = raw === null || raw === undefined ? "" : String(raw);
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
}

function updateSummary(summary) {
  const gen = fmtDt(summary.generated_at);
  const lat = fmtDt(summary.latest_dt_display || summary.latest_dt_close);
  $("summary").textContent = `生成：${gen} ｜ 最新：${lat}`;
}

function getParams() {
  const market = ($("marketSelect") && $("marketSelect").value) || "all";
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
    market, maPeriodClose, maFast, maSlow, rsiPeriod, rsiThreshold,
    emaPeriod, bollPeriod, bollStd, bollDownPeriod, bollDownStd, superAtrPeriod, superMult,
    kdjN, kdjM1, kdjM2, obvMaPeriod, stochRsiP, stochRsiK, stochRsiSmK, stochRsiSmD
  };
}

function computeBuiltins(row, params) {
  const closes = getSeries(row, "close");
  const out = {};
  const ps = [params.maPeriodClose, params.maFast, params.maSlow];
  for (const p of ps) {
    if (Number.isFinite(p) && p > 0) out[`ma_${p}`] = sma(closes, p);
  }
  if (Number.isFinite(params.rsiPeriod) && params.rsiPeriod > 0) out[`rsi_${params.rsiPeriod}`] = rsi(closes, params.rsiPeriod);
  return out;
}

function applyAllFilters(rows, params, customFactors) {
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

  const selected = [];
  let filteredOut = 0;
  let exprErrors = 0;

  for (const r of rows) {
    if (params.market !== "all" && String(r.market) !== String(params.market)) {
      continue;
    }
    r._builtins = computeBuiltins(r, params);
    r._expr = {};

    const closes = getSeries(r, "close");
    const highs = getSeries(r, "high");
    const lows = getSeries(r, "low");
    const volumes = getSeries(r, "volume");
    const lastClose = Number(r.close);

    if (enabledCloseMa) {
      const k = `ma_${params.maPeriodClose}`;
      const maV = r._builtins[k];
      if (maV === null || maV === undefined || !(lastClose > Number(maV))) {
        filteredOut++; continue;
      }
    }

    if (enabledMa) {
      const kf = `ma_${params.maFast}`;
      const ks = `ma_${params.maSlow}`;
      const maF = r._builtins[kf];
      const maS = r._builtins[ks];
      if (maF === null || maS === null || maF === undefined || maS === undefined || !(Number(maF) > Number(maS))) {
        filteredOut++; continue;
      }
    }

    if (enabledRsi) {
      const kr = `rsi_${params.rsiPeriod}`;
      const rv = r._builtins[kr];
      if (rv === null || rv === undefined || !(Number(rv) > Number(params.rsiThreshold))) {
        filteredOut++; continue;
      }
    }

    if (enabledEma) {
      const ev = ema(closes, params.emaPeriod);
      if (ev === null || !(lastClose > ev)) { filteredOut++; continue; }
    }

    if (enabledBollUp) {
      const ma = sma(closes, params.bollPeriod);
      const std = rollingStd(closes, params.bollPeriod);
      if (ma === null || std === null || !(lastClose > ma + params.bollStd * std)) { filteredOut++; continue; }
    }

    if (enabledBollDown) {
      const ma = sma(closes, params.bollDownPeriod);
      const std = rollingStd(closes, params.bollDownPeriod);
      if (ma === null || std === null || !(lastClose < ma - params.bollDownStd * std)) { filteredOut++; continue; }
    }

    if (enabledSuper) {
      const st = supertrend(highs, lows, closes, params.superAtrPeriod, params.superMult);
      if (st === null || !(lastClose > st)) { filteredOut++; continue; }
    }

    if (enabledKdj) {
      const { k, d } = kdj(highs, lows, closes, params.kdjN, params.kdjM1, params.kdjM2);
      if (k === null || d === null || !(k > d)) { filteredOut++; continue; }
    }

    if (enabledObv) {
      const { obv: ov, ma: om } = obvWithMa(closes, volumes, params.obvMaPeriod);
      if (ov === null || om === null || !(ov > om)) { filteredOut++; continue; }
    }

    if (enabledStochRsi) {
      const { k, d } = stochRsi(closes, params.stochRsiP, params.stochRsiK, params.stochRsiSmK, params.stochRsiSmD);
      if (k === null || d === null || !(k > d)) { filteredOut++; continue; }
    }

    let exprFail = false;
    for (const f of customFactors) {
      try {
        const template = f.template || f.expr || "";
        const expr = expandTemplate(template, f.params || []);
        const v = evalExpression(expr, r);
        r._expr[f.id] = v;
        if (!f.enabled) continue;
        if (f.thresholdEnabled) {
          if (!compare(v, f.cmp, f.threshold)) {
            exprFail = true;
            break;
          }
        } else {
          if (v === null || v === undefined || !Number.isFinite(Number(v)) || Number(v) === 0) {
            exprFail = true;
            break;
          }
        }
      } catch {
        exprErrors++;
        exprFail = true;
        break;
      }
    }
    if (exprFail) {
      filteredOut++;
      continue;
    }

    selected.push(r);
  }

  return { selected, filteredOut, exprErrors };
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
      renderFolderConditions();
      refresh();
    });
    row.appendChild(cb1);

    const cb2 = document.createElement("input");
    cb2.type = "checkbox";
    cb2.checked = !!f.show;
    cb2.addEventListener("change", () => {
      f.show = cb2.checked;
      saveCustomFactors(state.customFactors);
      refresh();
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
      renderFolderConditions();
      renderCustomFactorList();
      refresh();
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
        renderFolderConditions();
        refresh();
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
            refresh();
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
        renderFolderConditions();
        renderCustomFactorList();
        refresh();
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

function renderExprParams(count) {
  const el = $("exprParams");
  if (!el) return;
  el.innerHTML = "";
  if (!count) return;

  const vals = Array.isArray(state.exprDraftParams) ? state.exprDraftParams : [];
  const next = [];
  for (let i = 0; i < count; i++) next.push(Number.isFinite(Number(vals[i])) ? Number(vals[i]) : i + 1);
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
  updateFolderOptions();
  renderFolderConditions();
  renderCustomFactorList();
  setExprEditorOpen(false);
  refresh();
}

async function refresh(opts = {}) {
  try {
    const now = new Date();
    const hh = String(now.getHours()).padStart(2, "0");
    const mm = String(now.getMinutes()).padStart(2, "0");
    const isAuto = !!opts.auto;
    if (isAuto) setStatus(`自动更新中… ${hh}:${mm}`);
    else if (opts.manual) setStatus(`手动刷新… ${hh}:${mm}`);

    showProgress(8, "更新数据...");
    let backendTriggered = false;
    try {
      const r = await fetch("./api/refresh", { method: "POST" });
      if (!r.ok) throw new Error(`refresh api: ${r.status}`);
      backendTriggered = true;
    } catch (e) {
      if (opts.manual) {
        $("counts").textContent = "未能触发后端更新（静态模式/接口不可用），已改为仅重新读取快照";
        setStatus(`静态刷新… ${hh}:${mm}`);
      }
    }

    showProgress(18, "读取快照...");
    const latest = await loadJson("./data/latest.json");
    state.latest = latest;

    showProgress(40, "构建列与排序...");
    const params = getParams();
    const customFactors = state.customFactors;
    const displayFields = buildDisplayFields(params, customFactors);
    buildSortOptionsFromFields(displayFields, state.meta && state.meta.default_sort);
    buildTableHeader(displayFields);

    showProgress(65, "计算指标与筛选...");
    const allRows = Array.isArray(latest.results) ? latest.results : [];
    const { selected, exprErrors } = applyAllFilters(allRows, params, customFactors);

    showProgress(85, "排序与渲染...");
    const { sorted, sortKey } = sortRows(selected, displayFields);
    assignRank(sorted, sortKey, displayFields);
    renderTable(sorted, displayFields);

    const msg = exprErrors ? ` ｜ 表达式错误：${exprErrors}` : "";
    const total = params.market === "all" ? allRows.length : allRows.filter((r) => String(r.market) === String(params.market)).length;
    $("counts").textContent = `显示：${sorted.length} / ${total}${msg}`;
    updateSummary(latest.summary || {});

    showProgress(100, "完成");
    setTimeout(() => hideProgress(), 160);

    const hitText = sorted.length > 0 ? ` ｜ 命中：${sorted.length}` : " ｜ 命中：0";
    if (isAuto) setStatus(`自动更新完成 ${hh}:${mm}${hitText}`);
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
  const tick = () => {
    const now = new Date();
    const key = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, "0")}${String(now.getDate()).padStart(2, "0")}${String(now.getHours()).padStart(2, "0")}`;
    if (state.lastAutoUpdateKey === key) return;
    if (now.getMinutes() !== 0) return;
    state.lastAutoUpdateKey = key;
    refresh({ auto: true });
  };
  setInterval(tick, 10000);
  tick();
}

function initControls(meta) {
  const cfg = meta.config || {};

  // Theme toggle
  const themeToggle = $("themeToggle");
  const body = document.body;
  const themeIcon = $("themeIcon");

  const updateThemeUI = (isLight) => {
    if (isLight) {
      body.classList.add("light-theme");
      themeIcon.innerHTML = `<path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"></path>`; // Moon icon
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
      `; // Sun icon
    }
  };

  const savedTheme = localStorage.getItem("screener_theme");
  updateThemeUI(savedTheme === "light");

  themeToggle.addEventListener("click", () => {
    const isLight = body.classList.toggle("light-theme");
    localStorage.setItem("screener_theme", isLight ? "light" : "dark");
    updateThemeUI(isLight);
  });

  if ($("marketSelect")) $("marketSelect").value = "all";
  $("maPeriodClose").value = String(cfg.cond_ma_slow || 20);
  $("maFast").value = String(cfg.cond_ma_fast || 10);
  $("maSlow").value = String(cfg.cond_ma_slow || 20);
  $("rsiPeriod").value = String(cfg.cond_rsi_period || 14);
  $("rsiThreshold").value = Number(cfg.cond_rsi_threshold || 80);

  $("condCloseMa").checked = true;
  $("condMa").checked = true;
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

  $("btnRefresh").addEventListener("click", () => refresh({ manual: true }));
  $("sortKey").addEventListener("change", () => refresh());
  $("sortOrder").addEventListener("change", () => refresh());

  const ids = [
    "maPeriodClose", "maFast", "maSlow", "rsiPeriod", "rsiThreshold",
    "condCloseMa", "condMa", "condRsi", "marketSelect",
    "emaPeriod", "bollPeriod", "bollStd", "bollDownPeriod", "bollDownStd", "superAtrPeriod", "superMult",
    "kdjN", "kdjM1", "kdjM2", "obvMaPeriod", "stochRsiP", "stochRsiK", "stochRsiSmK", "stochRsiSmD",
    "condEma", "condBollUp", "condBollDown", "condSuper", "condKdj", "condObv", "condStochRsi"
  ];

  for (const id of ids) {
    const el = $(id);
    if (el) el.addEventListener("change", () => refresh());
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
    if (e.key === "Escape") setHelpOpen(false);
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
}

async function boot() {
  showProgress(8, "加载配置...");
  const meta = await loadJson("./data/meta.json");
  state.meta = meta;

  state.customFactors = loadCustomFactors();
  state.columnVisibility = loadColumnVisibility();
  updateFolderOptions();
  renderFolderConditions();
  renderCustomFactorList();
  setExprEditorOpen(false);
  renderExprParams(detectTemplateParamCount($("exprText") && $("exprText").value));
  syncExprThresholdUI();

  showProgress(35, "初始化界面...");
  initControls(meta);
  scheduleHourlyAutoUpdate();
  await refresh();
}

boot().catch((e) => {
  hideProgress();
  $("summary").textContent = String(e && e.message ? e.message : e);
});
