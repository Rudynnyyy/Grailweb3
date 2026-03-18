from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _http_post_json(url: str, *, payload: dict, cookie: str, timeout: float = 30.0) -> tuple[int, dict | None, str]:
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    req = Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Cookie": cookie,
        },
        method="POST",
    )
    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return 200, json.loads(raw), ""
    except HTTPError as e:
        try:
            raw = e.read().decode("utf-8", errors="replace")
        except Exception:
            raw = ""
        return int(e.code), (json.loads(raw) if raw else None), f"http_error: {e}"
    except URLError as e:
        return -1, None, f"url_error: {e}"
    except Exception as e:
        return -1, None, f"unknown_error: {e}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--python", type=str, default=sys.executable)
    ap.add_argument("--base-url", type=str, default="http://127.0.0.1:8001")
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument("--retries", type=int, default=8)
    args = ap.parse_args()

    repo = _repo_root()
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))
    from apps.db.auth_sqlite import AuthConfig, default_db_path, init_db, register_user, get_user_by_username_or_phone, create_session  # noqa: E402

    cfg = AuthConfig(db_path=default_db_path(repo))
    init_db(cfg)

    username = "bench"
    email = "bench@example.com"
    password = "bench123"
    ok, _ = register_user(cfg, username=username, email=email, phone=None, password=password)
    u = get_user_by_username_or_phone(cfg, identity=username)
    if not u:
        raise SystemExit("create_or_load_user_failed")
    sid = create_session(cfg, user_id=int(u["id"]))
    cookie = f"qc_sess={sid}"

    latest_p = repo / "apps" / "crypto_screener" / "web" / "data" / "latest.json"
    latest = json.loads(latest_p.read_text(encoding="utf-8"))
    rows = latest.get("results") if isinstance(latest, dict) else None
    rows = rows if isinstance(rows, list) else []
    picks = []
    seen = set()
    for r in rows:
        if not isinstance(r, dict):
            continue
        m = str(r.get("market") or "").strip().lower()
        s = str(r.get("symbol") or "").strip()
        k = f"{m}|{s}"
        if m in ("swap", "spot") and s and k not in seen:
            seen.add(k)
            picks.append({"market": m, "symbol": s})
        if len(picks) >= 3:
            break
    if not picks:
        raise SystemExit("no_picks_from_latest_json")

    url = f"{args.base_url.rstrip('/')}/api/latest_enriched"
    payload = {
        "custom_factors": [],
        "params": {"market": "all"},
        "toggles": {},
        "tail": 120,
        "chunk_offset": 0,
        "chunk_limit": 10,
        "fields": ["rank", "symbol", "market", "dt_display", "close", "pct_change"],
        "include_debug": False,
        "symbols": picks,
    }

    last = None
    for i in range(max(1, int(args.retries))):
        code, j, err = _http_post_json(url, payload=payload, cookie=cookie, timeout=float(args.timeout))
        last = {"code": code, "err": err, "json": j}
        if code == 200 and isinstance(j, dict) and j.get("ok"):
            res = j.get("results")
            res = res if isinstance(res, list) else []
            print(json.dumps({"ok": True, "code": 200, "got": len(res), "want": len(picks)}, ensure_ascii=False))
            return
        time.sleep(0.5 + 0.25 * i)

    print(json.dumps({"ok": False, "last": last}, ensure_ascii=False, indent=2))
    raise SystemExit(2)


if __name__ == "__main__":
    main()
