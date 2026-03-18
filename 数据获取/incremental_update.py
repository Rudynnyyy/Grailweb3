from __future__ import annotations

import argparse
import json
import os
import shutil
import time
import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from .factor_loader import load_config_from_yaml
    from .preprocess_fast import preprocess_all, read_csv_tail
except ImportError:
    from factor_loader import load_config_from_yaml
    from preprocess_fast import preprocess_all, read_csv_tail


@dataclass(frozen=True)
class UpdateResult:
    ok: bool
    started_at: str
    ended_at: str
    duration_seconds: float
    row_count: int
    file_count: int
    size_bytes: int
    md5: str
    details: dict[str, Any]


def _load_yaml(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    try:
        import yaml

        y = yaml.safe_load(text)
        return y if isinstance(y, dict) else {}
    except Exception:
        try:
            return json.loads(text)
        except Exception:
            return {}


def _log_jsonl(log_path: Path, payload: dict[str, Any]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")


def _md5_many(items: list[str]) -> str:
    import hashlib

    m = hashlib.md5()
    for x in sorted(items):
        m.update(x.encode("utf-8"))
    return m.hexdigest()


def _rollback_dirty_partitions(root: Path, dirty: list[str]) -> None:
    for rel in dirty:
        p = root / rel
        if p.exists():
            shutil.rmtree(p, ignore_errors=True)


def _parse_iso_dt(x: Any) -> datetime | None:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def infer_latest_preprocessed_hour(output_root: Path) -> pd.Timestamp | None:
    mf = output_root / "manifest.json"
    if not mf.exists():
        return None
    try:
        payload = json.loads(mf.read_text(encoding="utf-8"))
    except Exception:
        return None
    files = payload.get("files") if isinstance(payload, dict) else None
    if not isinstance(files, list) or not files:
        return None
    best: datetime | None = None
    for rec in files:
        if not isinstance(rec, dict):
            continue
        tr = rec.get("time_range")
        if not isinstance(tr, dict):
            continue
        end0 = _parse_iso_dt(tr.get("end"))
        if end0 is None:
            continue
        if best is None or end0 > best:
            best = end0
    if best is None:
        return None
    return pd.Timestamp(best).floor("h")


def _compute_catchup_window(
    *,
    latest_done: pd.Timestamp | None,
    source_latest: pd.Timestamp | None = None,
    now_utc: datetime,
    lag_hours: int,
    max_hours: int,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None, int]:
    lag = max(0, int(lag_hours))
    end_hour = pd.Timestamp(now_utc).floor("h") - pd.Timedelta(hours=lag)
    if source_latest is not None:
        end_hour = min(end_hour, pd.to_datetime(source_latest, utc=True).floor("h"))
    cap = max(1, int(max_hours))
    if latest_done is None:
        start_hour = end_hour - pd.Timedelta(hours=cap - 1)
        return start_hour, end_hour, cap
    start_hour = pd.to_datetime(latest_done, utc=True).floor("h") + pd.Timedelta(hours=1)
    if start_hour > end_hour:
        return None, None, 0
    hours = int((end_hour - start_hour) / pd.Timedelta(hours=1)) + 1
    if hours > cap:
        start_hour = end_hour - pd.Timedelta(hours=cap - 1)
        hours = cap
    return start_hour, end_hour, hours


def _infer_latest_hour_from_csv(path: Path) -> pd.Timestamp | None:
    try:
        df = read_csv_tail(path, tail=3000, extra=400)
    except Exception:
        return None
    if df is None or df.empty:
        return None
    cols = [str(c).strip() for c in list(df.columns)]
    if "candle_begin_time" not in cols:
        return None
    s = df["candle_begin_time"]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        dt = pd.to_datetime(s, errors="coerce", utc=True)
    ok = int(dt.notna().sum())
    n = int(len(dt))
    if n > 0 and ok < max(3, int(n * 0.2)):
        xs = pd.to_numeric(s, errors="coerce")
        if int(xs.notna().sum()) >= max(3, int(n * 0.8)):
            dt_ms = pd.to_datetime(xs, errors="coerce", utc=True, unit="ms")
            dt_s = pd.to_datetime(xs, errors="coerce", utc=True, unit="s")
            if int(dt_ms.notna().sum()) >= ok:
                dt = dt_ms
                ok = int(dt.notna().sum())
            if int(dt_s.notna().sum()) > ok:
                dt = dt_s
    if dt.notna().sum() <= 0:
        return None
    try:
        mx = dt.max()
    except Exception:
        return None
    if mx is None or (isinstance(mx, float) and pd.isna(mx)):
        return None
    return pd.to_datetime(mx, utc=True, errors="coerce").floor("h")


def infer_latest_source_hour(*, source_swap_dir: Path, source_spot_dir: Path, anchor_symbol: str = "BTC-USDT") -> pd.Timestamp | None:
    sym = str(anchor_symbol or "BTC-USDT").strip().upper()
    cands = [sym]
    if sym.endswith("-USDT"):
        cands.append(sym.replace("-", ""))
    elif sym.endswith("USDT"):
        cands.append(f"{sym[:-4]}-USDT")
    best: pd.Timestamp | None = None
    for base in (source_swap_dir, source_spot_dir):
        if not base.exists():
            continue
        picked = None
        for s in cands:
            p = base / f"{s}.csv"
            if p.exists() and p.is_file():
                picked = p
                break
        if picked is None:
            continue
        ts = _infer_latest_hour_from_csv(picked)
        if ts is not None and (best is None or ts > best):
            best = ts
    return best



def run_incremental_once(config_path: Path, *, target_hour: pd.Timestamp | None = None) -> UpdateResult:
    repo_root = Path(__file__).resolve().parents[1]
    data = _load_yaml(config_path)
    cfg = load_config_from_yaml(config_path, repo_root=repo_root)
    mon = data.get("monitoring", {}) if isinstance(data, dict) else {}
    log_path = Path(mon.get("log_file") or str(repo_root / "数据获取" / "logs" / "incremental_metrics.jsonl"))
    alert_path = Path(mon.get("alert_file") or str(repo_root / "数据获取" / "logs" / "incremental_alerts.jsonl"))

    if target_hour is None:
        now = datetime.now(timezone.utc)
        target_hour = pd.Timestamp(now).floor("h") - pd.Timedelta(hours=1)
    else:
        target_hour = pd.to_datetime(target_hour, utc=True).floor("h")

    started = datetime.now(timezone.utc)
    dirty_partitions: list[str] = []
    try:
        out = preprocess_all(cfg.preprocess, start_hour=target_hour, end_hour=target_hour)
        recs = out.get("records", [])
        file_count = len(recs)
        rows = int(sum(int(x.get("row_count", 0)) for x in recs))
        size_bytes = int(sum(int(x.get("size_bytes", 0)) for x in recs))
        dirty_partitions = sorted({str(x.get("partition", "")) for x in recs if x.get("partition")})
        md5_v = _md5_many([str(x.get("md5", "")) for x in recs])
        ended = datetime.now(timezone.utc)
        payload = {
            "ok": True,
            "scheduled_hour": target_hour.isoformat(),
            "started_at": started.isoformat(),
            "ended_at": ended.isoformat(),
            "duration_seconds": round((ended - started).total_seconds(), 6),
            "row_count": rows,
            "file_count": file_count,
            "size_bytes": size_bytes,
            "md5": md5_v,
        }
        _log_jsonl(log_path, payload)
        return UpdateResult(
            ok=True,
            started_at=payload["started_at"],
            ended_at=payload["ended_at"],
            duration_seconds=float(payload["duration_seconds"]),
            row_count=rows,
            file_count=file_count,
            size_bytes=size_bytes,
            md5=md5_v,
            details={"scheduled_hour": target_hour.isoformat(), "partitions": dirty_partitions},
        )
    except Exception as e:
        _rollback_dirty_partitions(cfg.preprocess.output_root, dirty_partitions)
        ended = datetime.now(timezone.utc)
        alarm = {
            "ok": False,
            "scheduled_hour": target_hour.isoformat(),
            "started_at": started.isoformat(),
            "ended_at": ended.isoformat(),
            "duration_seconds": round((ended - started).total_seconds(), 6),
            "error": str(e),
            "rolled_back_partitions": dirty_partitions,
        }
        _log_jsonl(alert_path, alarm)
        return UpdateResult(
            ok=False,
            started_at=alarm["started_at"],
            ended_at=alarm["ended_at"],
            duration_seconds=float(alarm["duration_seconds"]),
            row_count=0,
            file_count=0,
            size_bytes=0,
            md5="",
            details=alarm,
        )


def run_incremental_catchup(
    config_path: Path,
    *,
    lag_hours: int = 1,
    max_hours: int = 24,
) -> UpdateResult:
    repo_root = Path(__file__).resolve().parents[1]
    data = _load_yaml(config_path)
    cfg = load_config_from_yaml(config_path, repo_root=repo_root)
    mon = data.get("monitoring", {}) if isinstance(data, dict) else {}
    log_path = Path(mon.get("log_file") or str(repo_root / "数据获取" / "logs" / "incremental_metrics.jsonl"))
    alert_path = Path(mon.get("alert_file") or str(repo_root / "数据获取" / "logs" / "incremental_alerts.jsonl"))

    started = datetime.now(timezone.utc)
    dirty_partitions: list[str] = []
    try:
        latest_done = infer_latest_preprocessed_hour(cfg.preprocess.output_root)
        source_latest = infer_latest_source_hour(
            source_swap_dir=cfg.preprocess.source_swap_dir,
            source_spot_dir=cfg.preprocess.source_spot_dir,
            anchor_symbol=os.environ.get("QC_PREPROCESS_ANCHOR_SYMBOL") or "BTC-USDT",
        )
        start_hour, end_hour, hours = _compute_catchup_window(
            latest_done=latest_done,
            source_latest=source_latest,
            now_utc=started,
            lag_hours=int(lag_hours),
            max_hours=int(max_hours),
        )
        if start_hour is None or end_hour is None or hours <= 0:
            ended = datetime.now(timezone.utc)
            payload = {
                "ok": True,
                "scheduled_hour": None,
                "catchup": {
                    "lag_hours": int(lag_hours),
                    "latest_done": latest_done.isoformat() if latest_done is not None else None,
                    "source_latest": source_latest.isoformat() if source_latest is not None else None,
                    "output_root": str(cfg.preprocess.output_root),
                    "start_hour": None,
                    "end_hour": None,
                    "hours": 0,
                },
                "started_at": started.isoformat(),
                "ended_at": ended.isoformat(),
                "duration_seconds": round((ended - started).total_seconds(), 6),
                "row_count": 0,
                "file_count": 0,
                "size_bytes": 0,
                "md5": "",
            }
            _log_jsonl(log_path, payload)
            return UpdateResult(
                ok=True,
                started_at=payload["started_at"],
                ended_at=payload["ended_at"],
                duration_seconds=float(payload["duration_seconds"]),
                row_count=0,
                file_count=0,
                size_bytes=0,
                md5="",
                details={"catchup": payload["catchup"]},
            )

        out = preprocess_all(cfg.preprocess, start_hour=start_hour, end_hour=end_hour)
        recs = out.get("records", [])
        file_count = len(recs)
        rows = int(sum(int(x.get("row_count", 0)) for x in recs))
        size_bytes = int(sum(int(x.get("size_bytes", 0)) for x in recs))
        dirty_partitions = sorted({str(x.get("partition", "")) for x in recs if x.get("partition")})
        md5_v = _md5_many([str(x.get("md5", "")) for x in recs])
        ended = datetime.now(timezone.utc)
        payload = {
            "ok": True,
            "scheduled_hour": None,
            "catchup": {
                "lag_hours": int(lag_hours),
                "latest_done": latest_done.isoformat() if latest_done is not None else None,
                "source_latest": source_latest.isoformat() if source_latest is not None else None,
                "output_root": str(cfg.preprocess.output_root),
                "start_hour": pd.to_datetime(start_hour, utc=True).isoformat(),
                "end_hour": pd.to_datetime(end_hour, utc=True).isoformat(),
                "hours": int(hours),
            },
            "started_at": started.isoformat(),
            "ended_at": ended.isoformat(),
            "duration_seconds": round((ended - started).total_seconds(), 6),
            "row_count": rows,
            "file_count": file_count,
            "size_bytes": size_bytes,
            "md5": md5_v,
        }
        _log_jsonl(log_path, payload)
        return UpdateResult(
            ok=True,
            started_at=payload["started_at"],
            ended_at=payload["ended_at"],
            duration_seconds=float(payload["duration_seconds"]),
            row_count=rows,
            file_count=file_count,
            size_bytes=size_bytes,
            md5=md5_v,
            details={"catchup": payload["catchup"], "partitions": dirty_partitions},
        )
    except Exception as e:
        _rollback_dirty_partitions(cfg.preprocess.output_root, dirty_partitions)
        ended = datetime.now(timezone.utc)
        alarm = {
            "ok": False,
            "scheduled_hour": None,
            "catchup": {"lag_hours": int(lag_hours)},
            "started_at": started.isoformat(),
            "ended_at": ended.isoformat(),
            "duration_seconds": round((ended - started).total_seconds(), 6),
            "error": str(e),
            "rolled_back_partitions": dirty_partitions,
        }
        _log_jsonl(alert_path, alarm)
        return UpdateResult(
            ok=False,
            started_at=alarm["started_at"],
            ended_at=alarm["ended_at"],
            duration_seconds=float(alarm["duration_seconds"]),
            row_count=0,
            file_count=0,
            size_bytes=0,
            md5="",
            details=alarm,
        )


def sleep_to_next_minute_05() -> None:
    now = datetime.now()
    nxt = now.replace(second=0, microsecond=0)
    if nxt.minute >= 5:
        nxt = (nxt + timedelta(hours=1)).replace(minute=5)
    else:
        nxt = nxt.replace(minute=5)
    wait = max(0.5, (nxt - now).total_seconds())
    time.sleep(wait)


def run_scheduler_forever(config_path: Path, *, lag_hours: int = 1, max_hours: int = 24) -> None:
    while True:
        run_incremental_catchup(config_path, lag_hours=int(lag_hours), max_hours=int(max_hours))
        sleep_to_next_minute_05()


def build_cron_line(repo_root: Path, config_path: Path) -> str:
    script = repo_root / "数据获取" / "incremental_update.py"
    return f"5 * * * * /usr/bin/python3 {script} --config {config_path} --once --lag-hours 1"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default=str(Path(__file__).resolve().with_name("config.yaml")))
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--lag-hours", type=int, default=int(os.environ.get("QC_PREPROCESS_LAG_HOURS") or "1"))
    parser.add_argument("--max-hours", type=int, default=int(os.environ.get("QC_PREPROCESS_MAX_HOURS_PER_RUN") or "24"))
    parser.add_argument("--target-hour", type=str, default="")
    args = parser.parse_args()
    config_path = Path(args.config)
    if args.once:
        if str(args.target_hour or "").strip():
            th = pd.to_datetime(str(args.target_hour), utc=True, errors="coerce")
            res = run_incremental_once(config_path, target_hour=None if pd.isna(th) else th)
        else:
            res = run_incremental_catchup(config_path, lag_hours=int(args.lag_hours), max_hours=int(args.max_hours))
        print(json.dumps({"ok": res.ok, "rows": res.row_count, "files": res.file_count, "duration": res.duration_seconds, "details": res.details}, ensure_ascii=False))
        if not bool(res.ok):
            raise SystemExit(2)
        return
    run_scheduler_forever(config_path, lag_hours=int(args.lag_hours), max_hours=int(args.max_hours))


if __name__ == "__main__":
    main()
