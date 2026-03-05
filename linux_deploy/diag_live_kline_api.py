from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_latest(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        j = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = j.get("results") if isinstance(j, dict) else None
    return rows if isinstance(rows, list) else []


def _get_json(url: str, timeout: float, headers: dict[str, str] | None) -> tuple[int, dict | None, str]:
    req = Request(url, method="GET")
    for k, v in (headers or {}).items():
        if k and v:
            req.add_header(k, v)
    try:
        with urlopen(req, timeout=timeout) as resp:
            code = int(resp.getcode())
            body = resp.read().decode("utf-8", errors="ignore")
            try:
                return code, json.loads(body), body
            except Exception:
                return code, None, body
    except HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="ignore")
        except Exception:
            pass
        try:
            return int(e.code), json.loads(body), body
        except Exception:
            return int(e.code), None, body
    except URLError as e:
        return 0, None, str(e)
    except Exception as e:
        return -1, None, str(e)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", type=str, default="http://127.0.0.1:8001")
    parser.add_argument("--market", type=str, default="all", choices=["all", "swap", "spot"])
    parser.add_argument("--limit", type=int, default=3000)
    parser.add_argument("--tail", type=int, default=360)
    parser.add_argument("--timeout", type=float, default=6.0)
    parser.add_argument("--latest-path", type=str, default="")
    parser.add_argument("--cookie", type=str, default="")
    parser.add_argument("--cookie-file", type=str, default="")
    args = parser.parse_args()

    repo_root = _repo_root()
    base_url_raw = str(args.base_url or "")
    base_url = base_url_raw.strip().strip("`").strip().strip("'").strip('"').strip()
    if not base_url:
        base_url = "http://127.0.0.1:8001"
    cookie = str(args.cookie or "").strip()
    if not cookie and args.cookie_file:
        try:
            cookie = Path(args.cookie_file).read_text(encoding="utf-8").strip()
        except Exception:
            cookie = ""
    headers: dict[str, str] = {}
    if cookie:
        headers["Cookie"] = cookie
    latest_path = Path(args.latest_path).resolve() if args.latest_path else (repo_root / "apps" / "crypto_screener" / "web" / "data" / "latest.json")
    rows = _load_latest(latest_path)

    out = {
        "base_url": base_url,
        "base_url_raw": base_url_raw,
        "latest_path": str(latest_path),
        "cookie_set": bool(cookie),
        "rows_total": len(rows),
        "checked": 0,
        "ok": 0,
        "failed": 0,
        "failed_examples": [],
    }

    lim = max(1, int(args.limit))
    for r in rows[:lim]:
        if not isinstance(r, dict):
            continue
        m = str(r.get("market") or "").lower()
        s = str(r.get("symbol") or "")
        if m not in ("swap", "spot") or not s:
            continue
        if args.market != "all" and m != args.market:
            continue
        qs = urlencode({"market": m, "symbol": s, "tail": int(args.tail)})
        url = f"{base_url.rstrip('/')}/api/kline?{qs}"
        out["checked"] += 1
        code, j, body = _get_json(url, timeout=float(args.timeout), headers=headers)
        is_ok = code == 200 and isinstance(j, dict) and bool(j.get("ok"))
        if is_ok:
            out["ok"] += 1
            continue
        out["failed"] += 1
        if len(out["failed_examples"]) < 30:
            item = {
                "market": m,
                "symbol": s,
                "http_code": code,
                "api_ok": (j.get("ok") if isinstance(j, dict) else None),
                "error": (j.get("error") if isinstance(j, dict) else None),
                "message": (j.get("message") if isinstance(j, dict) else None),
            }
            if isinstance(j, dict) and "base_dir" in j:
                item["base_dir"] = j.get("base_dir")
            if isinstance(j, dict) and "symbol" in j:
                item["api_symbol"] = j.get("symbol")
            if not isinstance(j, dict):
                item["raw"] = body[:200]
            out["failed_examples"].append(item)

    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
