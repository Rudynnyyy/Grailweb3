from __future__ import annotations

import base64
import hashlib
import json
import urllib.error
import urllib.request


def send_image(*, webhook_url: str, image_bytes: bytes, timeout: int = 8) -> tuple[bool, str]:
    url = (webhook_url or "").strip()
    if not url.startswith("http"):
        return False, "bad_webhook_url"
    raw = image_bytes or b""
    if not raw:
        return False, "empty_image"
    b64 = base64.b64encode(raw).decode("ascii")
    md5 = hashlib.md5(raw).hexdigest()
    body = {"msgtype": "image", "image": {"base64": b64, "md5": md5}}
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=max(1, int(timeout))) as r:
            raw2 = r.read()
        try:
            j = json.loads(raw2.decode("utf-8", errors="ignore"))
        except Exception:
            j = None
        if isinstance(j, dict) and int(j.get("errcode") or 0) == 0:
            return True, "ok"
        if isinstance(j, dict):
            return False, str(j.get("errmsg") or j.get("errcode") or "send_failed")
        return False, "send_failed"
    except urllib.error.HTTPError as e:
        try:
            raw3 = e.read() or b""
            return False, raw3.decode("utf-8", errors="ignore") or str(e)
        except Exception:
            return False, str(e)
    except Exception as e:
        return False, str(e)


def send_markdown(*, webhook_url: str, content: str, timeout: int = 8) -> tuple[bool, str]:
    url = (webhook_url or "").strip()
    if not url.startswith("http"):
        return False, "bad_webhook_url"
    body = {
        "msgtype": "markdown",
        "markdown": {"content": str(content or "")},
    }
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=max(1, int(timeout))) as r:
            raw = r.read()
        try:
            j = json.loads(raw.decode("utf-8", errors="ignore"))
        except Exception:
            j = None
        if isinstance(j, dict) and int(j.get("errcode") or 0) == 0:
            return True, "ok"
        if isinstance(j, dict):
            return False, str(j.get("errmsg") or j.get("errcode") or "send_failed")
        return False, "send_failed"
    except urllib.error.HTTPError as e:
        try:
            raw = e.read() or b""
            return False, raw.decode("utf-8", errors="ignore") or str(e)
        except Exception:
            return False, str(e)
    except Exception as e:
        return False, str(e)
