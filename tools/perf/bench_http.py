from __future__ import annotations

import argparse
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any

import psutil
import requests


@dataclass(frozen=True)
class RequestResult:
    ok: bool
    code: int
    ms: float
    bytes_out: int


def _percentile(xs: list[float], p: float) -> float:
    if not xs:
        return 0.0
    ys = sorted(xs)
    k = int(max(0, min(len(ys) - 1, int(len(ys) * p) - 1)))
    return float(ys[k])


def _now_ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())


def _get_server_pid(sess: requests.Session, base_url: str, cookie: str) -> int | None:
    headers = {}
    if cookie:
        headers["Cookie"] = cookie
    try:
        r = sess.get(f"{base_url}/api/debug_env", headers=headers, timeout=10)
        if not r.ok:
            return None
        j = r.json()
    except Exception:
        return None
    try:
        pid = int(((j or {}).get("server") or {}).get("pid") or 0)
    except Exception:
        pid = 0
    return pid if pid > 0 else None


def _post_latest_enriched(sess: requests.Session, base_url: str, cookie: str, payload: dict, timeout_s: float) -> RequestResult:
    headers = {"Content-Type": "application/json"}
    if cookie:
        headers["Cookie"] = cookie
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    t0 = time.perf_counter()
    try:
        r = sess.post(f"{base_url}/api/latest_enriched", data=body, headers=headers, timeout=timeout_s)
        ms = (time.perf_counter() - t0) * 1000.0
        ok = bool(r.ok)
        return RequestResult(ok=ok, code=int(r.status_code), ms=float(ms), bytes_out=int(len(r.content or b"")))
    except Exception:
        ms = (time.perf_counter() - t0) * 1000.0
        return RequestResult(ok=False, code=-1, ms=float(ms), bytes_out=0)


def _get_kline(sess: requests.Session, base_url: str, cookie: str, *, market: str, symbol: str, tail: int, timeout_s: float) -> RequestResult:
    headers = {}
    if cookie:
        headers["Cookie"] = cookie
    url = f"{base_url}/api/kline?market={market}&symbol={symbol}&tail={int(tail)}"
    t0 = time.perf_counter()
    try:
        r = sess.get(url, headers=headers, timeout=timeout_s)
        ms = (time.perf_counter() - t0) * 1000.0
        ok = bool(r.ok)
        return RequestResult(ok=ok, code=int(r.status_code), ms=float(ms), bytes_out=int(len(r.content or b"")))
    except Exception:
        ms = (time.perf_counter() - t0) * 1000.0
        return RequestResult(ok=False, code=-1, ms=float(ms), bytes_out=0)


def run_stage(
    *,
    endpoint: str,
    base_url: str,
    cookie: str,
    users: int,
    requests_per_user: int,
    max_workers: int,
    timeout_s: float,
    payload: dict,
    kline_market: str,
    kline_symbol: str,
    kline_tail: int,
) -> dict[str, Any]:
    users_i = max(1, int(users))
    rpu = max(1, int(requests_per_user))
    total = users_i * rpu
    workers = max(1, min(int(max_workers), total))

    sess = requests.Session()
    pid = _get_server_pid(sess, base_url, cookie)
    proc = psutil.Process(pid) if pid else None
    cpu0 = None
    rss0 = None
    if proc:
        try:
            cpu0 = float(proc.cpu_percent(interval=None))
            rss0 = int(proc.memory_info().rss)
        except Exception:
            cpu0 = None
            rss0 = None

    t_start = time.perf_counter()

    def one_user(_i: int) -> list[RequestResult]:
        s2 = requests.Session()
        out: list[RequestResult] = []
        for _ in range(rpu):
            if endpoint == "kline":
                out.append(_get_kline(s2, base_url, cookie, market=kline_market, symbol=kline_symbol, tail=kline_tail, timeout_s=timeout_s))
            else:
                out.append(_post_latest_enriched(s2, base_url, cookie, payload, timeout_s))
        return out

    results: list[RequestResult] = []
    codes: dict[str, int] = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(one_user, i) for i in range(users_i)]
        for fut in as_completed(futs):
            try:
                arr = fut.result() or []
            except Exception:
                arr = [RequestResult(ok=False, code=-1, ms=0.0, bytes_out=0)]
            for r in arr:
                results.append(r)
                k = str(int(r.code))
                codes[k] = int(codes.get(k) or 0) + 1

    elapsed_s = max(1e-6, time.perf_counter() - t_start)
    lat_ms = [float(x.ms) for x in results]
    ok_n = sum(1 for x in results if x.ok)
    err_n = len(results) - ok_n

    cpu_pct = None
    rss_mb = None
    if proc:
        try:
            cpu_pct = float(proc.cpu_percent(interval=None))
            rss_mb = float(proc.memory_info().rss) / (1024.0 * 1024.0)
        except Exception:
            cpu_pct = None
            rss_mb = None

    return {
        "users": int(users_i),
        "requests_per_user": int(rpu),
        "total_requests": int(total),
        "concurrency_workers": int(workers),
        "elapsed_s": round(float(elapsed_s), 3),
        "qps": round(float(total) / float(elapsed_s), 3),
        "success": int(ok_n),
        "errors": int(err_n),
        "error_rate": round(float(err_n) / float(max(1, total)), 6),
        "latency_ms": {
            "avg": round(float(statistics.mean(lat_ms)) if lat_ms else 0.0, 3),
            "p50": round(_percentile(lat_ms, 0.50), 3),
            "p95": round(_percentile(lat_ms, 0.95), 3),
            "p99": round(_percentile(lat_ms, 0.99), 3),
            "max": round(float(max(lat_ms)) if lat_ms else 0.0, 3),
        },
        "codes": codes,
        "resource": {
            "server_pid": int(pid or 0),
            "proc_cpu_pct": round(float(cpu_pct), 3) if cpu_pct is not None else None,
            "proc_rss_mb": round(float(rss_mb), 3) if rss_mb is not None else None,
            "proc_rss_mb_delta": round(float((rss0 or 0) / (1024.0 * 1024.0) - float(rss_mb or 0.0)), 3) if (rss0 is not None and rss_mb is not None) else None,
            "cpu0": cpu0,
            "rss0": rss0,
        },
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", type=str, default="http://127.0.0.1:8001")
    ap.add_argument("--cookie", type=str, default="")
    ap.add_argument("--endpoint", type=str, default="latest_enriched", choices=["latest_enriched", "kline"])
    ap.add_argument("--vus", type=str, default="1,10,50,100")
    ap.add_argument("--requests-per-user", type=int, default=3)
    ap.add_argument("--max-workers", type=int, default=64)
    ap.add_argument("--timeout-s", type=float, default=25.0)
    ap.add_argument("--out", type=str, default="")
    ap.add_argument("--kline-market", type=str, default="swap")
    ap.add_argument("--kline-symbol", type=str, default="BTC-USDT")
    ap.add_argument("--kline-tail", type=int, default=360)
    args = ap.parse_args()

    base_url = str(args.base_url or "").rstrip("/")
    vus = []
    for part in str(args.vus or "").replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            vus.append(int(float(part)))
        except Exception:
            continue
    if not vus:
        vus = [1, 10, 50, 100]

    sess = requests.Session()
    payload = {"custom_factors": []}
    out = {
        "ts": _now_ts(),
        "endpoint": f"{base_url}/api/{'latest_enriched' if args.endpoint == 'latest_enriched' else 'kline'}",
        "max_workers": int(args.max_workers),
        "requests_per_user": int(args.requests_per_user),
        "stages": [],
    }
    for u in vus:
        st = run_stage(
            endpoint=str(args.endpoint),
            base_url=base_url,
            cookie=str(args.cookie or ""),
            users=int(u),
            requests_per_user=int(args.requests_per_user),
            max_workers=int(args.max_workers),
            timeout_s=float(args.timeout_s),
            payload=payload,
            kline_market=str(args.kline_market),
            kline_symbol=str(args.kline_symbol),
            kline_tail=int(args.kline_tail),
        )
        out["stages"].append(st)
        time.sleep(1.0)

    raw = json.dumps(out, ensure_ascii=False, indent=2)
    if str(args.out or "").strip():
        p = str(args.out).strip()
        with open(p, "w", encoding="utf-8") as f:
            f.write(raw)
    print(raw, flush=True)


if __name__ == "__main__":
    main()
