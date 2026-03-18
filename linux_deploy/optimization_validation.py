from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from urllib.error import URLError, HTTPError
from urllib.request import urlopen


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _run_json_script(py: str, script: Path, args: list[str]) -> dict:
    proc = subprocess.run([py, str(script), *args], capture_output=True, text=True)
    out = {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
        "json": None,
    }
    if proc.stdout.strip():
        try:
            out["json"] = json.loads(proc.stdout)
        except Exception:
            out["json"] = None
    return out


def _http_json(url: str, timeout: float = 8.0) -> dict:
    try:
        with urlopen(url, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        return {"ok": True, "status": 200, "json": json.loads(raw), "error": ""}
    except HTTPError as e:
        return {"ok": False, "status": int(e.code), "json": None, "error": f"http_error: {e}"}
    except URLError as e:
        return {"ok": False, "status": -1, "json": None, "error": f"url_error: {e}"}
    except Exception as e:
        return {"ok": False, "status": -1, "json": None, "error": f"unknown_error: {e}"}


def _load_json(p: Path) -> dict | None:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--python", type=str, default=sys.executable)
    ap.add_argument("--base-url", type=str, default="http://127.0.0.1:8001")
    ap.add_argument("--skip-http", action="store_true")
    ap.add_argument("--out", type=str, default="")
    args = ap.parse_args()

    repo = _repo_root()
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = Path(args.out).resolve() if args.out else (repo / "linux_deploy" / "reports" / f"optimization_validation_{now}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    diag_latest = repo / "linux_deploy" / "diag_latest_json.py"
    diag_flow = repo / "linux_deploy" / "diag_data_flow.py"
    bench_small = repo / "apps" / "crypto_screener" / "app" / "perf_outputs" / "http_bench_small.json"
    hotspots = repo / "apps" / "crypto_screener" / "app" / "perf_outputs" / "hotspots.json"

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "repo_root": str(repo),
        "checks": {},
        "summary": {"ok": True, "errors": [], "warnings": []},
        "goals": {
            "first_screen_p95_sec": 1.2,
            "enriched_p95_sec": 4.0,
            "enriched_p99_sec": 6.0,
            "enriched_504_ratio": 0.005,
            "cpu_peak_reduce_ratio": 0.30,
            "memory_reduce_ratio": 0.15,
        },
    }

    report["checks"]["diag_latest_json"] = _run_json_script(args.python, diag_latest, [])
    report["checks"]["diag_data_flow"] = _run_json_script(args.python, diag_flow, [])

    bench_j = _load_json(bench_small)
    if isinstance(bench_j, dict):
        stages = bench_j.get("stages") or []
        p95_list = []
        err_list = []
        for s in stages:
            try:
                p95_list.append(float((((s or {}).get("latency_ms") or {}).get("p95"))))
            except Exception:
                continue
            try:
                err_list.append(float((s or {}).get("error_rate")))
            except Exception:
                continue
        report["checks"]["baseline_http_bench"] = {
            "ok": True,
            "path": str(bench_small),
            "stages": len(stages),
            "p95_ms_max": max(p95_list) if p95_list else None,
            "error_rate_max": max(err_list) if err_list else None,
        }
    else:
        report["checks"]["baseline_http_bench"] = {"ok": False, "path": str(bench_small), "error": "load_failed"}
        report["summary"]["warnings"].append("baseline_http_bench_missing")

    hot_j = _load_json(hotspots)
    if isinstance(hot_j, dict):
        report["checks"]["profile_hotspots"] = {
            "ok": True,
            "path": str(hotspots),
            "topn": int(hot_j.get("topn") or 0),
            "first_hotspot": ((hot_j.get("hotspots") or [{}])[0] or {}).get("func"),
        }
    else:
        report["checks"]["profile_hotspots"] = {"ok": False, "path": str(hotspots), "error": "load_failed"}
        report["summary"]["warnings"].append("hotspots_missing")

    if not args.skip_http:
        status = _http_json(f"{args.base_url.rstrip('/')}/api/status")
        report["checks"]["http_status"] = status
        if not status["ok"]:
            report["summary"]["warnings"].append("http_status_unavailable")

    for k, v in report["checks"].items():
        if not bool((v or {}).get("ok")):
            if k in ("diag_latest_json", "diag_data_flow"):
                report["summary"]["ok"] = False
                report["summary"]["errors"].append(f"{k}_failed")

    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"ok": report["summary"]["ok"], "out": str(out_path)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
