from __future__ import annotations

import argparse
import json
import sys
from typing import Any


def _load(p: str) -> dict[str, Any]:
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def _pick_stage(payload: dict[str, Any], users: int | None) -> dict[str, Any] | None:
    stages = (payload or {}).get("stages") or []
    if not stages:
        return None
    if users is None:
        return stages[-1]
    best = None
    for st in stages:
        try:
            if int(st.get("users") or 0) == int(users):
                best = st
                break
        except Exception:
            continue
    return best or stages[-1]


def _num(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", type=str, required=True)
    ap.add_argument("--candidate", type=str, required=True)
    ap.add_argument("--users", type=int, default=0)
    ap.add_argument("--min-improve", type=float, default=0.10)
    ap.add_argument("--max-cpu-worse", type=float, default=1.00)
    ap.add_argument("--max-mem-worse", type=float, default=0.20)
    args = ap.parse_args()

    b = _load(args.baseline)
    c = _load(args.candidate)
    users = int(args.users) if int(args.users) > 0 else None
    sb = _pick_stage(b, users)
    sc = _pick_stage(c, users)
    if sb is None or sc is None:
        print(json.dumps({"ok": False, "reason": "missing stages"}, ensure_ascii=False))
        sys.exit(2)

    b_p95 = _num(((sb.get("latency_ms") or {}).get("p95")))
    c_p95 = _num(((sc.get("latency_ms") or {}).get("p95")))
    b_qps = _num(sb.get("qps"))
    c_qps = _num(sc.get("qps"))
    b_err = _num(sb.get("error_rate"))
    c_err = _num(sc.get("error_rate"))

    p95_improve = (b_p95 - c_p95) / b_p95 if b_p95 > 0 else 0.0
    qps_improve = (c_qps - b_qps) / b_qps if b_qps > 0 else 0.0
    err_improve = (b_err - c_err) / b_err if b_err > 0 else (1.0 if c_err == 0.0 and b_err > 0 else 0.0)

    b_cpu = ((sb.get("resource") or {}).get("proc_cpu_pct"))
    c_cpu = ((sc.get("resource") or {}).get("proc_cpu_pct"))
    b_mem = ((sb.get("resource") or {}).get("proc_rss_mb"))
    c_mem = ((sc.get("resource") or {}).get("proc_rss_mb"))

    cpu_worse = None
    if b_cpu is not None and c_cpu is not None and _num(b_cpu) > 0:
        cpu_worse = (_num(c_cpu) - _num(b_cpu)) / _num(b_cpu)
    mem_worse = None
    if b_mem is not None and c_mem is not None and _num(b_mem) > 0:
        mem_worse = (_num(c_mem) - _num(b_mem)) / _num(b_mem)

    ok_perf = (p95_improve >= float(args.min_improve)) or (qps_improve >= float(args.min_improve)) or (err_improve >= float(args.min_improve))
    ok_err = c_err <= b_err + 1e-9
    ok_cpu = True if cpu_worse is None else (cpu_worse <= float(args.max_cpu_worse))
    ok_mem = True if mem_worse is None else (mem_worse <= float(args.max_mem_worse))
    ok = bool(ok_perf and ok_err and ok_cpu and ok_mem)

    out = {
        "ok": ok,
        "stage_users": int(sc.get("users") or 0),
        "baseline": {"p95_ms": b_p95, "qps": b_qps, "error_rate": b_err, "cpu_pct": b_cpu, "rss_mb": b_mem},
        "candidate": {"p95_ms": c_p95, "qps": c_qps, "error_rate": c_err, "cpu_pct": c_cpu, "rss_mb": c_mem},
        "delta": {"p95_improve": round(p95_improve, 6), "qps_improve": round(qps_improve, 6), "err_improve": round(err_improve, 6), "cpu_worse": cpu_worse, "mem_worse": mem_worse},
        "thresholds": {"min_improve": float(args.min_improve), "max_cpu_worse": float(args.max_cpu_worse), "max_mem_worse": float(args.max_mem_worse)},
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
