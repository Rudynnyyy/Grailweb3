from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _http_get_json(url: str, *, cookie: str, timeout: float = 15.0) -> tuple[int, dict | None, str]:
    req = Request(url, headers={"Accept": "application/json", "Cookie": cookie}, method="GET")
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


def _http_post_json(url: str, *, payload: dict, cookie: str, timeout: float = 15.0) -> tuple[int, dict | None, str]:
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    req = Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json", "Cookie": cookie},
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
    ap.add_argument("--base-url", type=str, default="http://127.0.0.1:8001")
    ap.add_argument("--timeout", type=float, default=15.0)
    args = ap.parse_args()

    repo = _repo_root()
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))
    from apps.db.auth_sqlite import AuthConfig, default_db_path, init_db, register_user, get_user_by_username_or_phone, create_session  # noqa: E402

    cfg = AuthConfig(db_path=default_db_path(repo))
    init_db(cfg)
    username = "dualrun_bench"
    email = "dualrun_bench@example.com"
    password = "bench123"
    register_user(cfg, username=username, email=email, phone=None, password=password)
    u = get_user_by_username_or_phone(cfg, identity=username)
    if not u:
        raise SystemExit("create_or_load_user_failed")
    sid = create_session(cfg, user_id=int(u["id"]))
    cookie = f"qc_sess={sid}"
    base = args.base_url.rstrip("/")

    code0, j0, err0 = _http_get_json(f"{base}/api/dualrun_config", cookie=cookie, timeout=float(args.timeout))
    if code0 != 200 or not isinstance(j0, dict) or not j0.get("ok"):
        print(json.dumps({"ok": False, "step": "get_config", "code": code0, "err": err0, "json": j0}, ensure_ascii=False, indent=2))
        raise SystemExit(2)

    payload = {
        "enabled": True,
        "primary": "legacy",
        "sample_ratio": 0.25,
        "max_symbols": 320,
        "split_rule": "user_hash",
    }
    code1, j1, err1 = _http_post_json(f"{base}/api/dualrun_config", payload=payload, cookie=cookie, timeout=float(args.timeout))
    if code1 != 200 or not isinstance(j1, dict) or not j1.get("ok"):
        print(json.dumps({"ok": False, "step": "post_config", "code": code1, "err": err1, "json": j1}, ensure_ascii=False, indent=2))
        raise SystemExit(2)

    code2, j2, err2 = _http_get_json(f"{base}/api/dualrun_metrics", cookie=cookie, timeout=float(args.timeout))
    if code2 != 200 or not isinstance(j2, dict) or not j2.get("ok"):
        print(json.dumps({"ok": False, "step": "get_metrics", "code": code2, "err": err2, "json": j2}, ensure_ascii=False, indent=2))
        raise SystemExit(2)

    cfg2 = j1.get("config") if isinstance(j1, dict) else {}
    print(
        json.dumps(
            {
                "ok": True,
                "primary": cfg2.get("primary"),
                "sample_ratio": cfg2.get("sample_ratio"),
                "max_symbols": cfg2.get("max_symbols"),
                "metrics_keys": sorted(list((j2.get("metrics") or {}).keys())),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
