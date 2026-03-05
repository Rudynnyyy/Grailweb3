from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _b64e(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode("ascii").rstrip("=")


def _b64d(s: str) -> bytes:
    pad = "=" * ((4 - (len(s) % 4)) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("ascii"))


def _pbkdf2_hash(password: str, salt: bytes, *, rounds: int) -> bytes:
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, rounds, dklen=32)


def _username_ok(u: str) -> bool:
    if not u:
        return False
    if len(u) < 3 or len(u) > 32:
        return False
    for ch in u:
        if ch.isalnum():
            continue
        if ch in ("_", "-", "."):
            continue
        return False
    return True


def _password_ok(p: str) -> bool:
    if not p:
        return False
    return 6 <= len(p) <= 128


def _phone_ok(p: str) -> bool:
    s = (p or "").strip()
    if not s:
        return False
    if re.fullmatch(r"\+?\d{8,20}", s) is None:
        return False
    return True


def _email_ok(e: str) -> bool:
    s = (e or "").strip()
    if not s:
        return False
    if len(s) > 254:
        return False
    if re.fullmatch(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", s) is None:
        return False
    return True


@dataclass(frozen=True)
class AuthConfig:
    db_path: Path
    session_ttl_days: int = 14
    pbkdf2_rounds: int = 200_000
    sms_code_ttl_seconds: int = 300
    sms_send_cooldown_seconds: int = 60
    sms_send_limit_per_hour: int = 8
    email_code_ttl_seconds: int = 300
    email_send_cooldown_seconds: int = 60
    email_send_limit_per_hour: int = 8
    sms_code_pepper: str | None = None


def default_db_path(repo_root: Path) -> Path:
    return repo_root / "apps" / "crypto_screener" / "app" / "crypto_screener.sqlite3"


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(cfg: AuthConfig) -> None:
    conn = connect(cfg.db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              username TEXT NOT NULL UNIQUE,
              phone TEXT UNIQUE,
              email TEXT UNIQUE,
              salt_b64 TEXT NOT NULL,
              hash_b64 TEXT NOT NULL,
              rounds INTEGER NOT NULL,
              created_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
              id TEXT PRIMARY KEY,
              user_id INTEGER NOT NULL,
              created_at TEXT NOT NULL,
              expires_at TEXT NOT NULL,
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sms_codes (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              phone TEXT NOT NULL,
              scene TEXT NOT NULL,
              code_hash TEXT NOT NULL,
              created_at TEXT NOT NULL,
              expires_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS email_codes (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              email TEXT NOT NULL,
              scene TEXT NOT NULL,
              code_hash TEXT NOT NULL,
              created_at TEXT NOT NULL,
              expires_at TEXT NOT NULL
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sms_phone_scene ON sms_codes(phone,scene);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sms_expires_at ON sms_codes(expires_at);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_email_email_scene ON email_codes(email,scene);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_email_expires_at ON email_codes(expires_at);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);")
        cols = [r[1] for r in conn.execute("PRAGMA table_info(users)").fetchall()]
        if "phone" not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN phone TEXT;")
        if "email" not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN email TEXT;")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_phone_unique ON users(phone) WHERE phone IS NOT NULL;")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email_unique ON users(email) WHERE email IS NOT NULL;")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS wecom_configs (
              user_id INTEGER PRIMARY KEY,
              webhook_url TEXT NOT NULL,
              enabled INTEGER NOT NULL,
              top_n INTEGER NOT NULL,
              config_json TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_wecom_enabled ON wecom_configs(enabled);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_custom_factors (
              user_id INTEGER NOT NULL,
              factor_id TEXT NOT NULL,
              config_json TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY(user_id, factor_id),
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_user_custom_factors_user_id ON user_custom_factors(user_id);")
    finally:
        conn.close()


def register_user(cfg: AuthConfig, *, username: str, email: str, phone: str | None, password: str) -> tuple[bool, str]:
    u = (username or "").strip()
    em = (email or "").strip()
    ph = (phone or "").strip() if phone is not None else ""
    p = password or ""
    if not _username_ok(u):
        return False, "用户名不合法（3-32位：字母/数字/._-）"
    if not _email_ok(em):
        return False, "邮箱不合法"
    if ph and not _phone_ok(ph):
        return False, "手机号不合法"
    if not _password_ok(p):
        return False, "密码不合法（6-128位）"

    salt = secrets.token_bytes(16)
    rounds = int(cfg.pbkdf2_rounds)
    h = _pbkdf2_hash(p, salt, rounds=rounds)

    conn = connect(cfg.db_path)
    try:
        conn.execute(
            "INSERT INTO users(username,email,phone,salt_b64,hash_b64,rounds,created_at) VALUES(?,?,?,?,?,?,?)",
            (u, em, ph or None, _b64e(salt), _b64e(h), rounds, _utc_now().isoformat()),
        )
        return True, "ok"
    except sqlite3.IntegrityError:
        return False, "用户名或手机号或邮箱已存在"
    finally:
        conn.close()


def get_user_by_username_or_phone(cfg: AuthConfig, *, identity: str) -> dict | None:
    x = (identity or "").strip()
    if not x:
        return None
    conn = connect(cfg.db_path)
    try:
        row = conn.execute(
            "SELECT id,username,email,phone,salt_b64,hash_b64,rounds FROM users WHERE username=? OR email=? OR phone=?",
            (x, x, x),
        ).fetchone()
        if not row:
            return None
        uid, uname, email, phone, salt_b64, hash_b64, rounds = row
        return {
            "id": int(uid),
            "username": str(uname),
            "email": str(email) if email is not None else None,
            "phone": str(phone) if phone is not None else None,
            "salt_b64": str(salt_b64),
            "hash_b64": str(hash_b64),
            "rounds": int(rounds),
        }
    finally:
        conn.close()


def verify_user_identity_password(cfg: AuthConfig, *, identity: str, password: str) -> tuple[bool, int | None]:
    p = password or ""
    user = get_user_by_username_or_phone(cfg, identity=identity)
    if not user:
        return False, None
    salt = _b64d(user["salt_b64"])
    h0 = _b64d(user["hash_b64"])
    h = _pbkdf2_hash(p, salt, rounds=int(user["rounds"]))
    if secrets.compare_digest(h, h0):
        return True, int(user["id"])
    return False, None



def verify_user(cfg: AuthConfig, *, username: str, password: str) -> tuple[bool, int | None]:
    u = (username or "").strip()
    p = password or ""
    try:
        row = conn.execute("SELECT id,salt_b64,hash_b64,rounds FROM users WHERE username=?", (u,)).fetchone()
        if not row:
            return False, None
        user_id, salt_b64, hash_b64, rounds = row
        salt = _b64d(str(salt_b64))
        h0 = _b64d(str(hash_b64))
        h = _pbkdf2_hash(p, salt, rounds=int(rounds))
        if secrets.compare_digest(h, h0):
            return True, int(user_id)
        return False, None
    finally:
        conn.close()


def _sms_code_hash(cfg: AuthConfig, code: str, phone: str, scene: str) -> str:
    pepper = cfg.sms_code_pepper or os.environ.get("QC_SMS_PEPPER") or ""
    msg = f"{phone}|{scene}|{code}|{pepper}".encode("utf-8")
    return hashlib.sha256(msg).hexdigest()


def can_send_sms(cfg: AuthConfig, *, phone: str, scene: str) -> tuple[bool, str]:
    ph = (phone or "").strip()
    sc = (scene or "").strip()
    if not _phone_ok(ph):
        return False, "手机号不合法"
    if sc not in ("register", "login"):
        return False, "场景不合法"
    now = _utc_now()
    conn = connect(cfg.db_path)
    try:
        cleanup_sms(cfg)
        row = conn.execute(
            "SELECT created_at FROM sms_codes WHERE phone=? AND scene=? ORDER BY id DESC LIMIT 1",
            (ph, sc),
        ).fetchone()
        if row:
            try:
                last = datetime.fromisoformat(str(row[0]))
            except Exception:
                last = None
            if last and (now - last).total_seconds() < cfg.sms_send_cooldown_seconds:
                return False, "发送过于频繁"
        since = (now - timedelta(hours=1)).isoformat()
        cnt = conn.execute(
            "SELECT COUNT(1) FROM sms_codes WHERE phone=? AND created_at >= ?",
            (ph, since),
        ).fetchone()
        if cnt and int(cnt[0] or 0) >= int(cfg.sms_send_limit_per_hour):
            return False, "发送次数过多"
        return True, "ok"
    finally:
        conn.close()


def create_sms_code(cfg: AuthConfig, *, phone: str, scene: str) -> tuple[bool, str, str | None]:
    ph = (phone or "").strip()
    sc = (scene or "").strip()
    ok, msg = can_send_sms(cfg, phone=ph, scene=sc)
    if not ok:
        return False, msg, None
    code = f"{secrets.randbelow(1_000_000):06d}"
    now = _utc_now()
    exp = now + timedelta(seconds=max(60, int(cfg.sms_code_ttl_seconds)))
    h = _sms_code_hash(cfg, code, ph, sc)
    conn = connect(cfg.db_path)
    try:
        conn.execute(
            "INSERT INTO sms_codes(phone,scene,code_hash,created_at,expires_at) VALUES(?,?,?,?,?)",
            (ph, sc, h, now.isoformat(), exp.isoformat()),
        )
    finally:
        conn.close()
    return True, "ok", code


def verify_sms_code(cfg: AuthConfig, *, phone: str, scene: str, code: str) -> bool:
    ph = (phone or "").strip()
    sc = (scene or "").strip()
    cd = (code or "").strip()
    if not _phone_ok(ph):
        return False
    if sc not in ("register", "login"):
        return False
    if not re.fullmatch(r"\d{6}", cd or ""):
        return False
    now = _utc_now().isoformat()
    h = _sms_code_hash(cfg, cd, ph, sc)
    conn = connect(cfg.db_path)
    try:
        row = conn.execute(
            "SELECT id FROM sms_codes WHERE phone=? AND scene=? AND code_hash=? AND expires_at >= ? ORDER BY id DESC LIMIT 1",
            (ph, sc, h, now),
        ).fetchone()
        if not row:
            return False
        conn.execute("DELETE FROM sms_codes WHERE id=?", (int(row[0]),))
        return True
    finally:
        conn.close()


def cleanup_sms(cfg: AuthConfig) -> int:
    now = _utc_now().isoformat()
    conn = connect(cfg.db_path)
    try:
        cur = conn.execute("DELETE FROM sms_codes WHERE expires_at < ?", (now,))
        return int(cur.rowcount or 0)
    finally:
        conn.close()


def _email_code_hash(cfg: AuthConfig, code: str, email: str, scene: str) -> str:
    pepper = cfg.sms_code_pepper or os.environ.get("QC_SMS_PEPPER") or ""
    msg = f"{email}|{scene}|{code}|{pepper}".encode("utf-8")
    return hashlib.sha256(msg).hexdigest()


def can_send_email(cfg: AuthConfig, *, email: str, scene: str) -> tuple[bool, str]:
    em = (email or "").strip()
    sc = (scene or "").strip()
    if not _email_ok(em):
        return False, "邮箱不合法"
    if sc not in ("register", "login"):
        return False, "场景不合法"
    now = _utc_now()
    conn = connect(cfg.db_path)
    try:
        cleanup_email(cfg)
        row = conn.execute(
            "SELECT created_at FROM email_codes WHERE email=? AND scene=? ORDER BY id DESC LIMIT 1",
            (em, sc),
        ).fetchone()
        if row:
            try:
                last = datetime.fromisoformat(str(row[0]))
            except Exception:
                last = None
            if last and (now - last).total_seconds() < cfg.email_send_cooldown_seconds:
                return False, "发送过于频繁"
        since = (now - timedelta(hours=1)).isoformat()
        cnt = conn.execute(
            "SELECT COUNT(1) FROM email_codes WHERE email=? AND created_at >= ?",
            (em, since),
        ).fetchone()
        if cnt and int(cnt[0] or 0) >= int(cfg.email_send_limit_per_hour):
            return False, "发送次数过多"
        return True, "ok"
    finally:
        conn.close()


def create_email_code(cfg: AuthConfig, *, email: str, scene: str) -> tuple[bool, str, str | None]:
    em = (email or "").strip()
    sc = (scene or "").strip()
    ok, msg = can_send_email(cfg, email=em, scene=sc)
    if not ok:
        return False, msg, None
    code = f"{secrets.randbelow(1_000_000):06d}"
    now = _utc_now()
    exp = now + timedelta(seconds=max(60, int(cfg.email_code_ttl_seconds)))
    h = _email_code_hash(cfg, code, em, sc)
    conn = connect(cfg.db_path)
    try:
        conn.execute(
            "INSERT INTO email_codes(email,scene,code_hash,created_at,expires_at) VALUES(?,?,?,?,?)",
            (em, sc, h, now.isoformat(), exp.isoformat()),
        )
    finally:
        conn.close()
    return True, "ok", code


def verify_email_code(cfg: AuthConfig, *, email: str, scene: str, code: str) -> bool:
    em = (email or "").strip()
    sc = (scene or "").strip()
    cd = (code or "").strip()
    if not _email_ok(em):
        return False
    if sc not in ("register", "login"):
        return False
    if not re.fullmatch(r"\d{6}", cd or ""):
        return False
    now = _utc_now().isoformat()
    h = _email_code_hash(cfg, cd, em, sc)
    conn = connect(cfg.db_path)
    try:
        row = conn.execute(
            "SELECT id FROM email_codes WHERE email=? AND scene=? AND code_hash=? AND expires_at >= ? ORDER BY id DESC LIMIT 1",
            (em, sc, h, now),
        ).fetchone()
        if not row:
            return False
        conn.execute("DELETE FROM email_codes WHERE id=?", (int(row[0]),))
        return True
    finally:
        conn.close()


def cleanup_email(cfg: AuthConfig) -> int:
    now = _utc_now().isoformat()
    conn = connect(cfg.db_path)
    try:
        cur = conn.execute("DELETE FROM email_codes WHERE expires_at < ?", (now,))
        return int(cur.rowcount or 0)
    finally:
        conn.close()


def create_session(cfg: AuthConfig, *, user_id: int) -> str:
    sid = _b64e(secrets.token_bytes(24))
    now = _utc_now()
    exp = now + timedelta(days=max(1, int(cfg.session_ttl_days)))
    conn = connect(cfg.db_path)
    try:
        conn.execute(
            "INSERT INTO sessions(id,user_id,created_at,expires_at) VALUES(?,?,?,?)",
            (sid, int(user_id), now.isoformat(), exp.isoformat()),
        )
    finally:
        conn.close()
    return sid


def delete_session(cfg: AuthConfig, *, session_id: str) -> None:
    sid = str(session_id or "")
    if not sid:
        return
    conn = connect(cfg.db_path)
    try:
        conn.execute("DELETE FROM sessions WHERE id=?", (sid,))
    finally:
        conn.close()


def cleanup_sessions(cfg: AuthConfig) -> int:
    now = _utc_now().isoformat()
    conn = connect(cfg.db_path)
    try:
        cur = conn.execute("DELETE FROM sessions WHERE expires_at < ?", (now,))
        return int(cur.rowcount or 0)
    finally:
        conn.close()


def get_user_by_session(cfg: AuthConfig, *, session_id: str) -> dict | None:
    sid = str(session_id or "")
    if not sid:
        return None
    now = _utc_now().isoformat()
    conn = connect(cfg.db_path)
    try:
        row = conn.execute(
            """
            SELECT u.id,u.username,u.email,s.expires_at
            FROM sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.id=? AND s.expires_at >= ?
            """,
            (sid, now),
        ).fetchone()
        if not row:
            return None
        uid, uname, email, exp = row
        return {"id": int(uid), "username": str(uname), "email": (str(email) if email is not None else None), "expires_at": str(exp)}
    finally:
        conn.close()


def get_wecom_config(cfg: AuthConfig, *, user_id: int) -> dict | None:
    conn = connect(cfg.db_path)
    try:
        row = conn.execute(
            "SELECT webhook_url,enabled,top_n,config_json,updated_at FROM wecom_configs WHERE user_id=?",
            (int(user_id),),
        ).fetchone()
        if not row:
            return None
        webhook_url, enabled, top_n, config_json, updated_at = row
        return {
            "webhook_url": str(webhook_url),
            "enabled": bool(int(enabled) or 0),
            "top_n": int(top_n),
            "config": json.loads(str(config_json) or "{}"),
            "updated_at": str(updated_at),
        }
    finally:
        conn.close()


def upsert_wecom_config(cfg: AuthConfig, *, user_id: int, webhook_url: str, enabled: bool, top_n: int, config: dict) -> None:
    now = _utc_now().isoformat()
    url = (webhook_url or "").strip()
    top = int(top_n)
    top = max(1, min(200, top))
    payload = json.dumps(config or {}, ensure_ascii=False)
    conn = connect(cfg.db_path)
    try:
        conn.execute(
            """
            INSERT INTO wecom_configs(user_id, webhook_url, enabled, top_n, config_json, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
              webhook_url=excluded.webhook_url,
              enabled=excluded.enabled,
              top_n=excluded.top_n,
              config_json=excluded.config_json,
              updated_at=excluded.updated_at
            """,
            (int(user_id), url, 1 if enabled else 0, top, payload, now, now),
        )
    finally:
        conn.close()


def list_enabled_wecom_configs(cfg: AuthConfig) -> list[dict]:
    conn = connect(cfg.db_path)
    try:
        rows = conn.execute(
            "SELECT user_id, webhook_url, top_n, config_json FROM wecom_configs WHERE enabled=1",
        ).fetchall()
        out: list[dict] = []
        for user_id, webhook_url, top_n, config_json in rows or []:
            try:
                cfg0 = json.loads(str(config_json) or "{}")
            except Exception:
                cfg0 = {}
            out.append(
                {
                    "user_id": int(user_id),
                    "webhook_url": str(webhook_url),
                    "top_n": int(top_n),
                    "config": cfg0,
                }
            )
        return out
    finally:
        conn.close()


def list_custom_factors(cfg: AuthConfig, *, user_id: int) -> list[dict]:
    conn = connect(cfg.db_path)
    try:
        rows = conn.execute(
            "SELECT factor_id, config_json FROM user_custom_factors WHERE user_id=? ORDER BY updated_at DESC",
            (int(user_id),),
        ).fetchall()
        out: list[dict] = []
        for fid, config_json in rows or []:
            try:
                obj = json.loads(str(config_json) or "{}")
            except Exception:
                obj = {}
            if isinstance(obj, dict):
                if "id" not in obj:
                    obj["id"] = str(fid)
                out.append(obj)
        return out
    finally:
        conn.close()


def upsert_custom_factor(cfg: AuthConfig, *, user_id: int, factor: dict) -> tuple[bool, str]:
    if not isinstance(factor, dict):
        return False, "bad_factor"
    fid = str(factor.get("id") or "").strip()
    if not fid or len(fid) > 80:
        return False, "bad_factor_id"
    now = _utc_now().isoformat()
    payload = json.dumps(factor, ensure_ascii=False)
    conn = connect(cfg.db_path)
    try:
        conn.execute(
            """
            INSERT INTO user_custom_factors(user_id, factor_id, config_json, created_at, updated_at)
            VALUES(?,?,?,?,?)
            ON CONFLICT(user_id, factor_id) DO UPDATE SET
              config_json=excluded.config_json,
              updated_at=excluded.updated_at
            """,
            (int(user_id), fid, payload, now, now),
        )
        return True, "ok"
    finally:
        conn.close()


def replace_custom_factors(cfg: AuthConfig, *, user_id: int, factors: list[dict]) -> tuple[bool, str]:
    arr = factors if isinstance(factors, list) else []
    now = _utc_now().isoformat()
    conn = connect(cfg.db_path)
    try:
        conn.execute("DELETE FROM user_custom_factors WHERE user_id=?", (int(user_id),))
        for it in arr:
            if not isinstance(it, dict):
                continue
            fid = str(it.get("id") or "").strip()
            if not fid or len(fid) > 80:
                continue
            payload = json.dumps(it, ensure_ascii=False)
            conn.execute(
                "INSERT INTO user_custom_factors(user_id, factor_id, config_json, created_at, updated_at) VALUES(?,?,?,?,?)",
                (int(user_id), fid, payload, now, now),
            )
        return True, "ok"
    finally:
        conn.close()


def delete_custom_factor(cfg: AuthConfig, *, user_id: int, factor_id: str) -> None:
    fid = str(factor_id or "").strip()
    if not fid:
        return
    conn = connect(cfg.db_path)
    try:
        conn.execute("DELETE FROM user_custom_factors WHERE user_id=? AND factor_id=?", (int(user_id), fid))
    finally:
        conn.close()
