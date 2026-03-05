from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass


def _pct_encode(s: str) -> str:
    return urllib.parse.quote(s, safe="~")


def _canonicalize(params: dict[str, str]) -> str:
    items = sorted((k, params[k]) for k in params.keys())
    return "&".join(f"{_pct_encode(k)}={_pct_encode(v)}" for k, v in items)


def _sign(access_key_secret: str, canonicalized_query: str) -> str:
    string_to_sign = "GET&%2F&" + _pct_encode(canonicalized_query)
    key = (access_key_secret + "&").encode("utf-8")
    msg = string_to_sign.encode("utf-8")
    digest = hmac.new(key, msg, hashlib.sha1).digest()
    return base64.b64encode(digest).decode("ascii")


@dataclass(frozen=True)
class AliyunSmsConfig:
    access_key_id: str
    access_key_secret: str
    sign_name: str
    template_register: str
    template_login: str
    endpoint: str = "https://dysmsapi.aliyuncs.com/"


def load_from_env() -> AliyunSmsConfig | None:
    akid = (
        os.environ.get("QC_ALIYUN_SMS_PREFIX")
        or os.environ.get("QC_ALIYUN_ACCESS_KEY_ID")
        or os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_ID")
        or ""
    ).strip()
    aks = (
        os.environ.get("QC_ALIYUN_SMS_EKEY")
        or os.environ.get("QC_ALIYUN_ACCESS_KEY_SECRET")
        or os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
        or ""
    ).strip()
    sign = (os.environ.get("QC_ALIYUN_SMS_SIGN_NAME") or "").strip()
    t_reg = (os.environ.get("QC_ALIYUN_SMS_TEMPLATE_REGISTER") or "").strip()
    t_log = (os.environ.get("QC_ALIYUN_SMS_TEMPLATE_LOGIN") or "").strip()
    if not (akid and aks and sign and t_reg and t_log):
        return None
    return AliyunSmsConfig(
        access_key_id=akid,
        access_key_secret=aks,
        sign_name=sign,
        template_register=t_reg,
        template_login=t_log,
    )


def send_code(*, cfg: AliyunSmsConfig, phone: str, scene: str, code: str) -> tuple[bool, str]:
    template = cfg.template_register if scene == "register" else cfg.template_login
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    params = {
        "AccessKeyId": cfg.access_key_id,
        "Action": "SendSms",
        "Format": "JSON",
        "PhoneNumbers": phone,
        "RegionId": "cn-hangzhou",
        "SignName": cfg.sign_name,
        "SignatureMethod": "HMAC-SHA1",
        "SignatureNonce": str(int(time.time() * 1000)),
        "SignatureVersion": "1.0",
        "TemplateCode": template,
        "TemplateParam": json.dumps({"code": str(code)}, ensure_ascii=False),
        "Timestamp": ts,
        "Version": "2017-05-25",
    }
    canonical = _canonicalize(params)
    params["Signature"] = _sign(cfg.access_key_secret, canonical)
    url = cfg.endpoint.rstrip("/") + "/?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=8) as r:
            raw = r.read()
        data = json.loads(raw.decode("utf-8", errors="ignore"))
        if str(data.get("Code") or "") == "OK":
            return True, "ok"
        return False, str(data.get("Message") or "send_failed")
    except urllib.error.HTTPError as e:
        try:
            raw = e.read() or b""
            data = json.loads(raw.decode("utf-8", errors="ignore")) if raw else {}
            msg = str(data.get("Message") or "") or str(e)
            code2 = str(data.get("Code") or "") or str(e.code)
            return False, f"{code2}: {msg}"
        except Exception:
            return False, str(e)
    except Exception as e:
        return False, str(e)
