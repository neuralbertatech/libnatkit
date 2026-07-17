#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import shutil
import socket
import sqlite3
import sys
import tempfile
import threading
import time
import traceback
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable

REPO_ROOT = Path(__file__).resolve().parents[2]
NATVR_SRC = REPO_ROOT / "natVR" / "src"
if str(NATVR_SRC) not in sys.path:
    sys.path.insert(0, str(NATVR_SRC))

from natvr.kafka_train_validate import (
    PipelineCancelledError,
    parse_selected_channel_indexes,
    run_pipeline,
)
from natvr.reconstruct_session import DiscoveredRun, discover_runs
from natkit_auth_shared import (
    AuthenticatedSessionUser,
    ComputeSlotPolicy,
    SharedAuthStore,
    websocket_cookie_header,
)

LOG = logging.getLogger(__name__)

if TYPE_CHECKING:
    from websockets.asyncio.server import ServerConnection

TERMINAL_JOB_STATUSES = {"completed", "failed", "cancelled"}
RUNNING_JOB_STATUSES = {"running", "cancelling"}
HEARTBEAT_INTERVAL_S = 5.0
WORKER_STALE_AFTER_S = 15.0
WORKER_AUTO_RECOVER_AFTER_S = 45.0
SHARED_STATE_DB_POLL_INTERVAL_S = 0.25
CONTROLLED_WORKER_STATUSES = {"online", "draining"}
DEFAULT_DRAIN_WAIT_TIMEOUT_S = 30.0


@dataclass(slots=True)
class JobRecord:
    job_id: str
    job_type: str = "train_validate"
    owner_principal_id: str | None = None
    worker_id: str = ""
    thread_slot_id: str = ""
    priority: int = 0
    status: str = "pending"
    message: str = ""
    report: dict[str, Any] | None = None
    error: str | None = None
    traceback_text: str | None = None
    stop_requested: bool = False
    created_at_us: int = 0
    started_at_us: int | None = None
    finished_at_us: int | None = None
    sequence: int = 0
    queue_token: int = 0
    restart_count: int = 0


@dataclass(slots=True)
class ThreadSlotState:
    slot_id: str
    worker_id: str
    slot_index: int
    access_mode: str = "shared"
    dedicated_username: str | None = None
    queue: asyncio.PriorityQueue[tuple[int, int, str, int]] = field(
        default_factory=asyncio.PriorityQueue
    )
    current_job_id: str | None = None
    busy_intervals: deque[tuple[float, float | None]] = field(default_factory=deque)
    completed_jobs: int = 0
    failed_jobs: int = 0
    cancelled_jobs: int = 0


@dataclass(slots=True)
class WorkerNodeRecord:
    worker_id: str
    slot_count: int
    last_heartbeat_us: int
    source: str = "embedded"
    status: str = "online"
    session_id: str | None = None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the libnatkit-hosted WebSocket control plane for Kafka-backed "
            "ML run selection, training, and validation workflows."
        )
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8786)
    parser.add_argument("--broker", default="127.0.0.1:29092")
    parser.add_argument("--group-id", default="natkit-ml-control-plane")
    parser.add_argument("--scratch-root")
    parser.add_argument("--idle-timeout-s", type=float, default=2.0)
    parser.add_argument("--no-direct-assign", action="store_true")
    parser.add_argument("--partition", type=int, default=0)
    parser.add_argument("--metadata-json")
    parser.add_argument("--allow-missing-session-end", action="store_true")
    parser.add_argument("--post-roll-us", type=int, default=1_000_000)
    parser.add_argument("--rest-gesture", default="rest")
    parser.add_argument("--active-gesture", default="fist")
    parser.add_argument("--window-ms", type=int, default=200)
    parser.add_argument("--hop-ms", type=int, default=50)
    parser.add_argument("--vote-windows", type=int, default=5)
    parser.add_argument("--confidence-threshold", type=float, default=0.6)
    parser.add_argument("--min-hold-windows", type=int, default=2)
    parser.add_argument("--emit-only-during-cue-hold", action="store_true")
    parser.add_argument(
        "--selected-field",
        action="append",
        dest="selected_fields",
        default=[],
    )
    parser.add_argument("--worker-id")
    parser.add_argument("--worker-threads", type=int)
    parser.add_argument("--max-worker-restart-attempts", type=int, default=1)
    parser.add_argument("--state-json")
    parser.add_argument("--state-db")
    parser.add_argument("--default-principal-id", default="default")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def namespace_from_config(
    base_args: argparse.Namespace,
    overrides: dict[str, Any] | None = None,
) -> argparse.Namespace:
    payload = dict(vars(base_args))
    payload.update(overrides or {})
    no_direct_assign = bool(payload.get("no_direct_assign", False))
    payload["direct_assign"] = not no_direct_assign
    return argparse.Namespace(**payload)


def run_to_dict(run: DiscoveredRun) -> dict[str, Any]:
    return {
        "session_id": run.session_id,
        "run_index": run.run_index,
        "start_us": run.start_us,
        "end_us": run.end_us,
        "device_ids": list(run.device_ids),
        "purpose": run.purpose,
        "participant_id": run.participant_id,
        "protocol_id": run.protocol_id,
        "tags": list(run.tags),
        "notes": run.notes,
        "marker_count": run.marker_count,
        "last_activity_us": run.last_activity_us,
    }


def selector_for_run(run: dict[str, Any]) -> str:
    session_id = str(run["session_id"])
    run_index = int(run["run_index"])
    return f"{session_id}:{run_index}"


def normalize_run_selector(run: Any) -> str:
    # The UI submits train_runs/eval_runs as "session:run" strings; discovery
    # helpers produce run dicts. Accept either.
    if isinstance(run, str):
        return run
    return selector_for_run(run)


def build_pipeline_namespace(
    base_args: argparse.Namespace,
    payload: dict[str, Any],
    *,
    output_dir: str,
) -> argparse.Namespace:
    train_runs = [normalize_run_selector(run) for run in payload.get("train_runs") or []]
    eval_runs = [normalize_run_selector(run) for run in payload.get("eval_runs") or []]
    overrides = {
        "broker": payload.get("broker", base_args.broker),
        "output_dir": output_dir,
        "families": payload.get("families"),
        "train_runs": train_runs,
        "eval_runs": eval_runs,
        "selected_fields": list(payload.get("selected_fields") or []),
        "rest_gesture": payload.get("rest_gesture", base_args.rest_gesture),
        "active_gesture": payload.get("active_gesture", base_args.active_gesture),
        "window_ms": int(payload.get("window_ms", base_args.window_ms)),
        "hop_ms": int(payload.get("hop_ms", base_args.hop_ms)),
        "vote_windows": int(payload.get("vote_windows", base_args.vote_windows)),
        "confidence_threshold": float(
            payload.get("confidence_threshold", base_args.confidence_threshold)
        ),
        "min_hold_windows": int(
            payload.get("min_hold_windows", base_args.min_hold_windows)
        ),
        "emit_only_during_cue_hold": bool(
            payload.get(
                "emit_only_during_cue_hold",
                base_args.emit_only_during_cue_hold,
            )
        ),
    }
    return namespace_from_config(base_args, overrides)


def create_job_workspace(base_args: argparse.Namespace) -> Path:
    scratch_root = getattr(base_args, "scratch_root", None)
    if scratch_root:
        root_path = Path(scratch_root)
        root_path.mkdir(parents=True, exist_ok=True)
        return Path(tempfile.mkdtemp(prefix="natkit-ml-", dir=root_path))
    return Path(tempfile.mkdtemp(prefix="natkit-ml-"))


def sanitize_replay_report(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "session_id": report.get("session_id"),
        "expected_windows": report.get("expected_windows"),
        "predicted_windows": report.get("predicted_windows"),
        "matched_windows": report.get("matched_windows"),
        "missing_windows": report.get("missing_windows"),
        "unmatched_predictions": report.get("unmatched_predictions"),
        "coverage": report.get("coverage"),
        "accuracy": report.get("accuracy"),
        "confusion_matrix": report.get("confusion_matrix"),
    }


def resolve_artifacts_dir() -> Path:
    # Durable, backend-accessible location for trained model artifacts (Phase 5,
    # slice C). In the dev stack this is a volume shared read-only with the
    # backend so a classify node can load model_path directly.
    return Path(os.getenv("NATKIT_ML_ARTIFACTS_DIR", "/models"))


def persist_selected_model_artifact(
    report: dict[str, Any], job_id: str
) -> str | None:
    """Copy the winning model out of the ephemeral job workspace into the durable
    artifacts directory before the workspace is deleted. Returns the durable path
    (valid on the shared volume) or None if there is nothing to persist. Only the
    in-process control-plane worker can do this; remote-worker artifacts stay on
    the worker's filesystem (a documented follow-up)."""
    source = report.get("selected_model_path")
    if not source:
        return None
    source_path = Path(str(source))
    if not source_path.is_file():
        return None
    dest_dir = resolve_artifacts_dir() / job_id
    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / source_path.name
        shutil.copy2(source_path, dest_path)
    except OSError as exc:
        LOG.warning("Could not persist model artifact for job %s: %s", job_id, exc)
        return None
    return str(dest_path)


def persist_bundle_artifact(report: dict[str, Any], job_id: str) -> str | None:
    """Copy the self-describing emg-gesture bundle (the artifact the live C++
    emg_gesture_classify transform loads) into the durable artifacts directory,
    alongside the model. Returns the durable bundle path or None."""
    source = report.get("bundle_path")
    if not source:
        return None
    source_path = Path(str(source))
    if not source_path.is_file():
        return None
    dest_dir = resolve_artifacts_dir() / job_id
    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / source_path.name
        shutil.copy2(source_path, dest_path)
    except OSError as exc:
        LOG.warning("Could not persist bundle artifact for job %s: %s", job_id, exc)
        return None
    return str(dest_path)


def sanitize_pipeline_report(
    report: dict[str, Any],
    model_path: str | None = None,
    bundle_path: str | None = None,
) -> dict[str, Any]:
    return {
        "broker": report.get("broker"),
        "artifact_storage": "durable" if model_path else "ephemeral_scratch",
        "model_path": model_path,
        # Durable path to the live-inference bundle. Auto-filled into an
        # emg_gesture_classify node's model_path on the frontend so live
        # classification uses the guaranteed-parity artifact.
        "bundle_path": bundle_path,
        "model_family": report.get("selected_family"),
        "selected_fields": list(report.get("selected_fields") or []),
        "selected_channel_indexes": list(report.get("selected_channel_indexes") or []),
        "train_runs": [
            {
                "session_id": run.get("session_id"),
                "run_index": run.get("run_index"),
                "device_id": run.get("device_id"),
            }
            for run in report.get("train_runs", [])
        ],
        "eval_runs": [
            {
                "session_id": run.get("session_id"),
                "run_index": run.get("run_index"),
                "device_id": run.get("device_id"),
            }
            for run in report.get("eval_runs", [])
        ],
        "results": [
            {
                "family": result.get("family"),
                "mean_accuracy": result.get("mean_accuracy"),
                "min_accuracy": result.get("min_accuracy"),
                "mean_coverage": result.get("mean_coverage"),
                "replay_reports": [
                    sanitize_replay_report(replay)
                    for replay in result.get("replay_reports", [])
                ],
            }
            for result in report.get("results", [])
        ],
        "selected_family": report.get("selected_family"),
        "selected_mean_accuracy": report.get("selected_mean_accuracy"),
        "selected_min_accuracy": report.get("selected_min_accuracy"),
        "selected_mean_coverage": report.get("selected_mean_coverage"),
    }


def build_job_request_payload(
    base_args: argparse.Namespace,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "broker": payload.get("broker", base_args.broker),
        "families": list(payload.get("families") or []),
        "train_runs": list(payload.get("train_runs") or []),
        "eval_runs": list(payload.get("eval_runs") or []),
        "selected_fields": list(payload.get("selected_fields") or []),
        "rest_gesture": payload.get("rest_gesture", base_args.rest_gesture),
        "active_gesture": payload.get("active_gesture", base_args.active_gesture),
        "window_ms": int(payload.get("window_ms", base_args.window_ms)),
        "hop_ms": int(payload.get("hop_ms", base_args.hop_ms)),
        "vote_windows": int(payload.get("vote_windows", base_args.vote_windows)),
        "confidence_threshold": float(
            payload.get("confidence_threshold", base_args.confidence_threshold)
        ),
        "min_hold_windows": int(
            payload.get("min_hold_windows", base_args.min_hold_windows)
        ),
        "emit_only_during_cue_hold": bool(
            payload.get(
                "emit_only_during_cue_hold",
                base_args.emit_only_during_cue_hold,
            )
        ),
    }


def now_us() -> int:
    return int(time.time() * 1_000_000)


def normalize_principal_id(value: Any) -> str | None:
    if value is None:
        return None
    principal_id = str(value).strip()
    return principal_id or None


def default_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


def resolve_worker_thread_count(explicit: int | None) -> int:
    if explicit is not None:
        return max(0, int(explicit))
    detected = os.cpu_count() or 1
    return max(1, detected)


def build_thread_slot_id(worker_id: str, slot_index: int) -> str:
    return f"{worker_id}:slot-{slot_index + 1:02d}"


def load_scheduler_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def open_scheduler_state_db(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=5.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA busy_timeout=5000")
    return connection


def init_scheduler_state_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open_scheduler_state_db(path) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS scheduler_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS scheduler_slots (
                slot_id TEXT PRIMARY KEY,
                slot_index INTEGER NOT NULL,
                worker_id TEXT NOT NULL,
                owner_principal_id TEXT,
                access_mode TEXT NOT NULL DEFAULT 'shared',
                dedicated_username TEXT
            )
            """
        )
        slot_columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(scheduler_slots)").fetchall()
        }
        if "access_mode" not in slot_columns:
            connection.execute(
                "ALTER TABLE scheduler_slots ADD COLUMN access_mode TEXT NOT NULL DEFAULT 'shared'"
            )
        if "dedicated_username" not in slot_columns:
            connection.execute(
                "ALTER TABLE scheduler_slots ADD COLUMN dedicated_username TEXT"
            )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS scheduler_jobs (
                job_id TEXT PRIMARY KEY,
                job_type TEXT NOT NULL,
                owner_principal_id TEXT,
                worker_id TEXT NOT NULL,
                thread_slot_id TEXT NOT NULL,
                priority INTEGER NOT NULL,
                status TEXT NOT NULL,
                message TEXT NOT NULL,
                request_json TEXT,
                report_json TEXT,
                error TEXT,
                traceback_text TEXT,
                stop_requested INTEGER NOT NULL,
                created_at_us INTEGER NOT NULL,
                started_at_us INTEGER,
                finished_at_us INTEGER,
                sequence INTEGER NOT NULL,
                queue_token INTEGER NOT NULL,
                restart_count INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        job_columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(scheduler_jobs)").fetchall()
        }
        if "request_json" not in job_columns:
            connection.execute(
                "ALTER TABLE scheduler_jobs ADD COLUMN request_json TEXT"
            )
        if "restart_count" not in job_columns:
            connection.execute(
                "ALTER TABLE scheduler_jobs ADD COLUMN restart_count INTEGER NOT NULL DEFAULT 0"
            )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS scheduler_workers (
                worker_id TEXT PRIMARY KEY,
                slot_count INTEGER NOT NULL,
                last_heartbeat_us INTEGER NOT NULL,
                source TEXT NOT NULL,
                status TEXT NOT NULL,
                session_id TEXT
            )
            """
        )
        worker_columns = {
            str(row[1])
            for row in connection.execute(
                "PRAGMA table_info(scheduler_workers)"
            ).fetchall()
        }
        if "session_id" not in worker_columns:
            connection.execute(
                "ALTER TABLE scheduler_workers ADD COLUMN session_id TEXT"
            )
        connection.commit()


def load_scheduler_state_db(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    init_scheduler_state_db(path)
    with open_scheduler_state_db(path) as connection:
        metadata = {
            str(key): value
            for key, value in connection.execute(
                "SELECT key, value FROM scheduler_metadata"
            ).fetchall()
        }
        slots = [
            {
                "slot_id": slot_id,
                "slot_index": slot_index,
                "worker_id": worker_id,
                "owner_principal_id": owner_principal_id,
                "access_mode": access_mode,
                "dedicated_username": dedicated_username,
            }
            for slot_id, slot_index, worker_id, owner_principal_id, access_mode, dedicated_username in connection.execute(
                """
                SELECT slot_id, slot_index, worker_id, owner_principal_id, access_mode, dedicated_username
                FROM scheduler_slots
                ORDER BY slot_index ASC
                """
            ).fetchall()
        ]
        workers = [
            {
                "worker_id": worker_id,
                "slot_count": slot_count,
                "last_heartbeat_us": last_heartbeat_us,
                "source": source,
                "status": status,
                "session_id": session_id,
            }
            for worker_id, slot_count, last_heartbeat_us, source, status, session_id in connection.execute(
                """
                SELECT worker_id, slot_count, last_heartbeat_us, source, status, session_id
                FROM scheduler_workers
                ORDER BY worker_id ASC
                """
            ).fetchall()
        ]
        jobs = []
        for row in connection.execute(
            """
            SELECT
                job_id,
                job_type,
                owner_principal_id,
                worker_id,
                thread_slot_id,
                priority,
                status,
                message,
                request_json,
                report_json,
                error,
                traceback_text,
                stop_requested,
                created_at_us,
                started_at_us,
                finished_at_us,
                sequence,
                queue_token
                ,
                restart_count
            FROM scheduler_jobs
            ORDER BY sequence ASC, created_at_us ASC
            """
        ).fetchall():
            (
                job_id,
                job_type,
                owner_principal_id,
                worker_id,
                thread_slot_id,
                priority,
                status,
                message,
                request_json,
                report_json,
                error,
                traceback_text,
                stop_requested,
                created_at_us,
                started_at_us,
                finished_at_us,
                sequence,
                queue_token,
                restart_count,
            ) = row
            jobs.append(
                {
                    "job_id": job_id,
                    "job_type": job_type,
                    "owner_principal_id": owner_principal_id,
                    "worker_id": worker_id,
                    "thread_slot_id": thread_slot_id,
                    "priority": priority,
                    "status": status,
                    "message": message,
                    "request": json.loads(request_json) if request_json else None,
                    "report": json.loads(report_json) if report_json else None,
                    "error": error,
                    "traceback_text": traceback_text,
                    "stop_requested": bool(stop_requested),
                    "created_at_us": created_at_us,
                    "started_at_us": started_at_us,
                    "finished_at_us": finished_at_us,
                    "sequence": sequence,
                    "queue_token": queue_token,
                    "restart_count": restart_count,
                }
            )
    return {"metadata": metadata, "workers": workers, "slots": slots, "jobs": jobs}


def load_scheduler_state_db_revision(path: Path) -> int:
    if not path.exists():
        return 0
    init_scheduler_state_db(path)
    with open_scheduler_state_db(path) as connection:
        row = connection.execute(
            "SELECT value FROM scheduler_metadata WHERE key = 'state_revision'"
        ).fetchone()
    if row is None:
        return 0
    try:
        return max(0, int(row[0]))
    except (TypeError, ValueError):
        return 0


def stable_json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True)


def sync_sqlite_table_rows(
    connection: sqlite3.Connection,
    *,
    table: str,
    key_column: str,
    value_columns: tuple[str, ...],
    rows: list[tuple[Any, ...]],
) -> None:
    existing_keys = {
        row[0]
        for row in connection.execute(f"SELECT {key_column} FROM {table}").fetchall()
    }
    desired_keys = {row[0] for row in rows}
    stale_keys = sorted(existing_keys - desired_keys)
    if stale_keys:
        connection.executemany(
            f"DELETE FROM {table} WHERE {key_column} = ?",
            [(key,) for key in stale_keys],
        )

    insert_columns = ", ".join((key_column, *value_columns))
    insert_placeholders = ", ".join("?" for _ in range(len(value_columns) + 1))
    update_assignments = ", ".join(
        f"{column} = excluded.{column}" for column in value_columns
    )
    unchanged_predicate = " AND ".join(
        f"{column} IS excluded.{column}" for column in value_columns
    )
    connection.executemany(
        (
            f"INSERT INTO {table} ({insert_columns}) VALUES ({insert_placeholders}) "
            f"ON CONFLICT({key_column}) DO UPDATE SET {update_assignments} "
            f"WHERE NOT ({unchanged_predicate})"
        ),
        rows,
    )


def persist_scheduler_state_db(
    path: Path,
    *,
    worker_id: str,
    slot_count: int,
    default_principal_id: str,
    workers: list[dict[str, Any]],
    slots: list[dict[str, Any]],
    jobs: list[dict[str, Any]],
) -> int:
    init_scheduler_state_db(path)
    with open_scheduler_state_db(path) as connection:
        existing_revision_row = connection.execute(
            "SELECT value FROM scheduler_metadata WHERE key = 'state_revision'"
        ).fetchone()
        existing_revision = 0
        if existing_revision_row is not None:
            try:
                existing_revision = int(existing_revision_row[0])
            except (TypeError, ValueError):
                existing_revision = 0
        next_revision = existing_revision + 1
        sync_sqlite_table_rows(
            connection,
            table="scheduler_metadata",
            key_column="key",
            value_columns=("value",),
            rows=[
                ("worker_id", worker_id),
                ("slot_count", str(slot_count)),
                ("default_principal_id", default_principal_id),
                ("state_revision", str(next_revision)),
                ("saved_at_us", str(now_us())),
            ],
        )
        sync_sqlite_table_rows(
            connection,
            table="scheduler_workers",
            key_column="worker_id",
            value_columns=(
                "slot_count",
                "last_heartbeat_us",
                "source",
                "status",
                "session_id",
            ),
            rows=[
                (
                    worker["worker_id"],
                    int(worker.get("slot_count") or 0),
                    int(worker.get("last_heartbeat_us") or 0),
                    str(worker.get("source") or "external"),
                    str(worker.get("status") or "online"),
                    worker.get("session_id"),
                )
                for worker in workers
            ],
        )
        sync_sqlite_table_rows(
            connection,
            table="scheduler_slots",
            key_column="slot_id",
            value_columns=(
                "slot_index",
                "worker_id",
                "owner_principal_id",
                "access_mode",
                "dedicated_username",
            ),
            rows=[
                (
                    slot["slot_id"],
                    slot["slot_index"],
                    slot["worker_id"],
                    slot.get("owner_principal_id"),
                    str(slot.get("access_mode") or "shared"),
                    slot.get("dedicated_username"),
                )
                for slot in slots
            ],
        )
        sync_sqlite_table_rows(
            connection,
            table="scheduler_jobs",
            key_column="job_id",
            value_columns=(
                "job_type",
                "owner_principal_id",
                "worker_id",
                "thread_slot_id",
                "priority",
                "status",
                "message",
                "request_json",
                "report_json",
                "error",
                "traceback_text",
                "stop_requested",
                "created_at_us",
                "started_at_us",
                "finished_at_us",
                "sequence",
                "queue_token",
                "restart_count",
            ),
            rows=[
                (
                    job["job_id"],
                    job.get("job_type") or "train_validate",
                    job.get("owner_principal_id"),
                    job["worker_id"],
                    job["thread_slot_id"],
                    int(job.get("priority") or 0),
                    job["status"],
                    job.get("message") or "",
                    stable_json_dumps(job["request"])
                    if job.get("request") is not None
                    else None,
                    stable_json_dumps(job["report"])
                    if job.get("report") is not None
                    else None,
                    job.get("error"),
                    job.get("traceback_text"),
                    1 if job.get("stop_requested") else 0,
                    int(job.get("created_at_us") or 0),
                    job.get("started_at_us"),
                    job.get("finished_at_us"),
                    int(job.get("sequence") or 0),
                    int(job.get("queue_token") or 0),
                    int(job.get("restart_count") or 0),
                )
                for job in jobs
            ],
        )
        connection.commit()
    return next_revision


def scheduler_state_revision(payload: dict[str, Any]) -> int:
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        return 0
    for key in ("state_revision", "saved_at_us"):
        value = metadata.get(key)
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            continue
    return 0


def compute_busy_ratio(
    intervals: Iterable[tuple[float, float | None]],
    *,
    now: float | None = None,
    window_s: float = 60.0,
) -> float:
    effective_now = now if now is not None else time.monotonic()
    window_start = effective_now - window_s
    busy_s = 0.0
    for start, end in intervals:
        interval_end = effective_now if end is None else end
        if interval_end <= window_start or start >= effective_now:
            continue
        busy_s += max(0.0, min(interval_end, effective_now) - max(start, window_start))
    if window_s <= 0:
        return 0.0
    return max(0.0, min(1.0, busy_s / window_s))


def job_to_dict(job: JobRecord) -> dict[str, Any]:
    return {
        "job_id": job.job_id,
        "job_type": job.job_type,
        "owner_principal_id": job.owner_principal_id,
        "worker_id": job.worker_id,
        "thread_slot_id": job.thread_slot_id,
        "priority": job.priority,
        "status": job.status,
        "message": job.message,
        "report": job.report,
        "error": job.error,
        "traceback_text": job.traceback_text,
        "stop_requested": job.stop_requested,
        "created_at_us": job.created_at_us,
        "started_at_us": job.started_at_us,
        "finished_at_us": job.finished_at_us,
        "restart_count": job.restart_count,
    }


def worker_status_from_heartbeat(
    *,
    worker_id: str,
    embedded_worker_id: str,
    last_heartbeat_us: int,
    status: str,
    now_wall_us: int | None = None,
    stale_after_s: float = WORKER_STALE_AFTER_S,
) -> str:
    normalized_status = str(status or "online").strip() or "online"
    if worker_id == embedded_worker_id:
        return normalized_status
    if normalized_status not in {"online", "starting"}:
        return normalized_status
    effective_now_us = now_wall_us if now_wall_us is not None else now_us()
    if last_heartbeat_us <= 0:
        return "stalled"
    stale_after_us = max(1, int(stale_after_s * 1_000_000))
    if effective_now_us - last_heartbeat_us >= stale_after_us:
        return "stalled"
    return normalized_status


def merge_worker_status(
    existing_status: str | None,
    reported_status: str | None,
) -> str:
    normalized_existing = str(existing_status or "online").strip() or "online"
    normalized_reported = str(reported_status or "online").strip() or "online"
    if normalized_existing == "draining" and normalized_reported in {"online", "starting"}:
        return "draining"
    return normalized_reported


RECOVERABLE_WORKER_STATUSES = {"stalled", "missing", "offline"}


def worker_status_allows_recovery(status: str) -> bool:
    return status in RECOVERABLE_WORKER_STATUSES


def recovered_worker_status_from_job_error(
    worker_id: str,
    error: str | None,
) -> str | None:
    if not isinstance(error, str):
        return None
    prefix = f"worker {worker_id} became "
    if not error.startswith(prefix):
        return None
    recovered_status = error[len(prefix) :].strip()
    if recovered_status in RECOVERABLE_WORKER_STATUSES:
        return recovered_status
    return None


def worker_is_past_auto_recover_deadline(
    *,
    worker_id: str,
    embedded_worker_id: str,
    last_heartbeat_us: int,
    now_wall_us: int | None = None,
    auto_recover_after_s: float = WORKER_AUTO_RECOVER_AFTER_S,
) -> bool:
    if worker_id == embedded_worker_id:
        return False
    if last_heartbeat_us <= 0:
        return True
    effective_now_us = now_wall_us if now_wall_us is not None else now_us()
    auto_recover_after_us = max(1, int(auto_recover_after_s * 1_000_000))
    return effective_now_us - last_heartbeat_us >= auto_recover_after_us


def slot_to_dict(
    slot: ThreadSlotState,
    jobs: dict[str, JobRecord],
    *,
    now_monotonic: float | None = None,
) -> dict[str, Any]:
    slot_all_jobs = [job for job in jobs.values() if job.thread_slot_id == slot.slot_id]
    slot_jobs = [
        job
        for job in slot_all_jobs
        if job.status not in TERMINAL_JOB_STATUSES
    ]
    queued_jobs = [job for job in slot_jobs if job.status == "queued"]
    running_jobs = [job for job in slot_jobs if job.status in RUNNING_JOB_STATUSES]
    return {
        "slot_id": slot.slot_id,
        "worker_id": slot.worker_id,
        "slot_index": slot.slot_index,
        "owner_principal_id": slot.dedicated_username,
        "access_mode": slot.access_mode,
        "dedicated_username": slot.dedicated_username,
        "current_job_id": slot.current_job_id,
        "queue_depth": len(queued_jobs),
        "assigned_job_count": len(slot_jobs),
        "running_job_count": len(running_jobs),
        "busy_ratio_60s": compute_busy_ratio(
            slot.busy_intervals, now=now_monotonic, window_s=60.0
        ),
        "completed_jobs": slot.completed_jobs,
        "failed_jobs": slot.failed_jobs,
        "cancelled_jobs": slot.cancelled_jobs,
        "restart_attempt_count": sum(
            max(0, int(job.restart_count)) for job in slot_all_jobs
        ),
    }


def worker_to_dict(
    worker_id: str,
    slots: Iterable[ThreadSlotState],
    jobs: dict[str, JobRecord],
    *,
    last_heartbeat_us: int,
    principal_id: str | None = None,
    viewer_has_shared_compute_access: bool = False,
    viewer_is_admin: bool = False,
    show_all: bool = False,
    now_monotonic: float | None = None,
    now_wall_us: int | None = None,
    slot_count_override: int | None = None,
    worker_source: str = "embedded",
    worker_status: str = "online",
    embedded_worker_id: str | None = None,
) -> dict[str, Any]:
    worker_slots = sorted(
        [slot for slot in slots if slot.worker_id == worker_id],
        key=lambda item: item.slot_index,
    )
    visible_slots = [
        slot
        for slot in worker_slots
        if principal_id is None
        or (viewer_is_admin and show_all)
        or (
            slot.access_mode == "shared"
            and (viewer_has_shared_compute_access or viewer_is_admin)
        )
        or (
            slot.access_mode == "dedicated"
            and slot.dedicated_username == principal_id
        )
    ]
    visible_slot_ids = {slot.slot_id for slot in visible_slots}
    worker_slot_ids = {slot.slot_id for slot in worker_slots}
    visible_slot_jobs = [
        job
        for job in jobs.values()
        if job.thread_slot_id in visible_slot_ids
    ]
    worker_slot_jobs = [
        job for job in jobs.values() if job.thread_slot_id in worker_slot_ids
    ]
    worker_active_jobs = [
        job for job in worker_slot_jobs if job.status not in TERMINAL_JOB_STATUSES
    ]
    slot_jobs = [
        job for job in visible_slot_jobs if job.status not in TERMINAL_JOB_STATUSES
    ]
    queued_job_count = sum(1 for job in slot_jobs if job.status == "queued")
    running_job_count = sum(
        1 for job in slot_jobs if job.status in RUNNING_JOB_STATUSES
    )
    assigned_job_count = len(slot_jobs)
    recovered_job_count = sum(
        1
        for job in jobs.values()
        if job.worker_id == worker_id
        and job.status == "failed"
        and recovered_worker_status_from_job_error(worker_id, job.error) is not None
    )
    busy_ratio_60s = 0.0
    if visible_slots:
        busy_ratio_60s = sum(
            compute_busy_ratio(slot.busy_intervals, now=now_monotonic, window_s=60.0)
            for slot in visible_slots
        ) / len(visible_slots)
    advertised_slot_count = len(worker_slots)
    if slot_count_override is not None:
        advertised_slot_count = max(advertised_slot_count, int(slot_count_override))
    resolved_worker_status = worker_status_from_heartbeat(
        worker_id=worker_id,
        embedded_worker_id=embedded_worker_id or worker_id,
        last_heartbeat_us=last_heartbeat_us,
        status=worker_status,
        now_wall_us=now_wall_us,
    )
    if (
        resolved_worker_status in RECOVERABLE_WORKER_STATUSES
        and running_job_count == 0
        and recovered_job_count > 0
    ):
        resolved_worker_status = "degraded"
    drain_remaining_job_count = len(worker_active_jobs)
    return {
        "worker_id": worker_id,
        "slot_count": advertised_slot_count,
        "visible_slot_count": len(visible_slots),
        "assigned_slot_count": sum(
            1 for slot in worker_slots if slot.access_mode == "dedicated"
        ),
        "queued_job_count": queued_job_count,
        "running_job_count": running_job_count,
        "assigned_job_count": assigned_job_count,
        "recovered_job_count": recovered_job_count,
        "restart_attempt_count": sum(
            max(0, int(job.restart_count)) for job in visible_slot_jobs
        ),
        "busy_ratio_60s": busy_ratio_60s,
        "last_heartbeat_us": last_heartbeat_us,
        "worker_source": worker_source,
        "worker_status": resolved_worker_status,
        "slot_inventory_ready": bool(worker_slots),
        "accepting_new_jobs": (
            bool(worker_slots) and resolved_worker_status not in {"draining", "offline", "missing", "stalled"}
        ),
        "drain_remaining_job_count": drain_remaining_job_count,
        "drain_ready": (
            resolved_worker_status == "draining" and drain_remaining_job_count == 0
        ),
    }


class MlControlPlaneServer:
    def __init__(self, base_args: argparse.Namespace) -> None:
        self._base_args = base_args
        self._jobs: dict[str, JobRecord] = {}
        self._jobs_lock = asyncio.Lock()
        self._clients: set[ServerConnection] = set()
        self._client_users: dict[ServerConnection, AuthenticatedSessionUser | None] = {}
        self._job_sequence = 0
        self._job_payloads: dict[str, tuple[argparse.Namespace, Path]] = {}
        self._job_requests: dict[str, dict[str, Any]] = {}
        self._job_stop_events: dict[str, threading.Event] = {}

        self._worker_id = base_args.worker_id or default_worker_id()
        self._slot_count = resolve_worker_thread_count(base_args.worker_threads)
        self._max_worker_restart_attempts = max(
            0, int(getattr(base_args, "max_worker_restart_attempts", 1))
        )
        self._default_principal_id = str(base_args.default_principal_id)
        self._auth_store = SharedAuthStore()
        self._state_path = Path(base_args.state_json) if getattr(base_args, "state_json", None) else None
        self._state_db_path = Path(base_args.state_db) if getattr(base_args, "state_db", None) else None
        self._slots: dict[str, ThreadSlotState] = {}
        self._workers: dict[str, WorkerNodeRecord] = {}
        self._slot_workers: list[asyncio.Task[None]] = []
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._worker_last_heartbeat_us = now_us()
        self._state_db_revision = 0
        self._last_state_db_refresh_check_monotonic = 0.0
        self._workers[self._worker_id] = WorkerNodeRecord(
            worker_id=self._worker_id,
            slot_count=self._slot_count,
            last_heartbeat_us=self._worker_last_heartbeat_us,
            source="embedded",
            status="online",
        )

        for slot_index in range(self._slot_count):
            slot = ThreadSlotState(
                slot_id=build_thread_slot_id(self._worker_id, slot_index),
                worker_id=self._worker_id,
                slot_index=slot_index,
                access_mode="shared",
                dedicated_username=None,
            )
            self._slots[slot.slot_id] = slot

        self._load_state()

    def _authenticated_user_payload(
        self, user: AuthenticatedSessionUser | None
    ) -> dict[str, Any] | None:
        if user is None:
            return None
        return {
            "username": user.username,
            "display_name": user.display_name,
            "is_admin": user.is_admin,
            "shared_compute_access": user.shared_compute_access,
        }

    def _slot_is_visible_to_user(
        self,
        slot: ThreadSlotState,
        user: AuthenticatedSessionUser | None,
        *,
        show_all: bool = False,
    ) -> bool:
        if user is None:
            return True
        if user.is_admin and show_all:
            return True
        if slot.access_mode == "dedicated":
            return slot.dedicated_username == user.username or user.is_admin
        return user.shared_compute_access or user.is_admin

    def _slot_is_submittable_by_user(
        self,
        slot: ThreadSlotState,
        user: AuthenticatedSessionUser | None,
    ) -> bool:
        if user is None:
            return False
        if user.is_admin:
            return True
        if slot.access_mode == "dedicated":
            return slot.dedicated_username == user.username
        return user.shared_compute_access

    def _apply_compute_slot_policies(
        self,
        policies: list[ComputeSlotPolicy],
    ) -> None:
        policies_by_slot = {policy.slot_id: policy for policy in policies}
        for slot in self._slots.values():
            policy = policies_by_slot.get(slot.slot_id)
            if policy is None:
                slot.access_mode = "shared"
                slot.dedicated_username = None
                continue
            slot.access_mode = (
                "dedicated" if policy.access_mode == "dedicated" else "shared"
            )
            slot.dedicated_username = (
                policy.dedicated_username if slot.access_mode == "dedicated" else None
            )

    def _connection_user(
        self, websocket: ServerConnection
    ) -> AuthenticatedSessionUser | None:
        return self._client_users.get(websocket)

    def _resolve_scheduler_principal(
        self,
        websocket: ServerConnection,
        payload: dict[str, Any],
        *,
        allow_all_for_admin: bool = False,
    ) -> str | None:
        user = self._connection_user(websocket)
        if user is not None:
            if allow_all_for_admin and user.is_admin and bool(payload.get("show_all")):
                return None
            return user.username
        return normalize_principal_id(payload.get("principal_id"))

    async def _require_authenticated_user(
        self, websocket: ServerConnection, request_id: Any
    ) -> AuthenticatedSessionUser | None:
        user = self._connection_user(websocket)
        if user is not None:
            return user
        await self._send_error(
            websocket,
            "authentication required",
            request_id=request_id,
        )
        return None

    async def _require_admin_user(
        self, websocket: ServerConnection, request_id: Any
    ) -> AuthenticatedSessionUser | None:
        user = await self._require_authenticated_user(websocket, request_id)
        if user is None:
            return None
        if user.is_admin:
            return user
        await self._send_error(
            websocket,
            "admin access required",
            request_id=request_id,
        )
        return None

    async def start(self) -> None:
        for slot in self._slots.values():
            if slot.worker_id != self._worker_id:
                continue
            self._slot_workers.append(asyncio.create_task(self._run_slot(slot)))
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    def _build_persist_payload(self) -> dict[str, Any]:
        jobs = [
            job_to_dict(job)
            | {
                "sequence": job.sequence,
                "queue_token": job.queue_token,
                "request": self._job_requests.get(job.job_id),
            }
            for job in self._jobs.values()
        ]
        workers = [
            {
                "worker_id": worker.worker_id,
                "slot_count": worker.slot_count,
                "last_heartbeat_us": worker.last_heartbeat_us,
                "source": worker.source,
                "status": worker.status,
                "session_id": worker.session_id,
            }
            for worker in sorted(self._workers.values(), key=lambda item: item.worker_id)
        ]
        slots = [
            {
                "slot_id": slot.slot_id,
                "slot_index": slot.slot_index,
                "worker_id": slot.worker_id,
                "owner_principal_id": slot.dedicated_username,
                "access_mode": slot.access_mode,
                "dedicated_username": slot.dedicated_username,
            }
            for slot in sorted(
                self._slots.values(),
                key=lambda item: (item.worker_id, item.slot_index),
            )
        ]
        return {
            "worker_id": self._worker_id,
            "slot_count": self._slot_count,
            "default_principal_id": self._default_principal_id,
            "saved_at_us": now_us(),
            "workers": workers,
            "slots": slots,
            "jobs": jobs,
        }

    def _write_persist_payload(self, payload: dict[str, Any]) -> None:
        if self._state_path is not None:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            self._state_path.write_text(
                json.dumps(payload, indent=2) + "\n",
                encoding="utf-8",
            )
        if self._state_db_path is not None:
            self._state_db_revision = persist_scheduler_state_db(
                self._state_db_path,
                worker_id=self._worker_id,
                slot_count=self._slot_count,
                default_principal_id=self._default_principal_id,
                workers=list(payload.get("workers") or []),
                slots=list(payload.get("slots") or []),
                jobs=list(payload.get("jobs") or []),
            )
            self._last_state_db_refresh_check_monotonic = time.monotonic()

    @staticmethod
    def _job_differs_from_persisted_payload(
        job: JobRecord,
        job_payload: dict[str, Any],
    ) -> bool:
        return (
            job.status != str(job_payload.get("status") or "failed")
            or job.message != str(job_payload.get("message") or "")
            or job.finished_at_us != job_payload.get("finished_at_us")
        )

    def _load_state(self) -> None:
        payload: dict[str, Any] = {}
        state_source: Path | None = None
        if self._state_db_path is not None:
            try:
                payload = load_scheduler_state_db(self._state_db_path)
                state_source = self._state_db_path
            except Exception as exc:  # pragma: no cover - defensive load path
                LOG.warning(
                    "failed to load scheduler state from %s: %s",
                    self._state_db_path,
                    exc,
                )
        elif self._state_path is not None:
            try:
                payload = load_scheduler_state(self._state_path)
                state_source = self._state_path
            except Exception as exc:  # pragma: no cover - defensive load path
                LOG.warning(
                    "failed to load scheduler state from %s: %s",
                    self._state_path,
                    exc,
                )
        if not payload:
            return
        self._state_db_revision = scheduler_state_revision(payload)
        state_changed_on_load = False

        for worker_payload in payload.get("workers", []):
            worker_id = str(worker_payload.get("worker_id") or "").strip()
            if not worker_id or worker_id == self._worker_id:
                continue
            slot_count = int(worker_payload.get("slot_count") or 0)
            if slot_count <= 0:
                continue
            self._workers[worker_id] = WorkerNodeRecord(
                worker_id=worker_id,
                slot_count=slot_count,
                last_heartbeat_us=int(worker_payload.get("last_heartbeat_us") or 0),
                source=str(worker_payload.get("source") or "external"),
                status=str(worker_payload.get("status") or "online"),
                session_id=normalize_principal_id(worker_payload.get("session_id")),
            )

        for slot_payload in payload.get("slots", []):
            slot_id = str(slot_payload.get("slot_id") or "")
            slot = self._slots.get(slot_id)
            worker_id = str(slot_payload.get("worker_id") or self._worker_id)
            slot_index = int(slot_payload.get("slot_index") or 0)
            if slot is None and worker_id != self._worker_id:
                slot = ThreadSlotState(
                    slot_id=slot_id,
                    worker_id=worker_id,
                    slot_index=slot_index,
                )
                self._slots[slot_id] = slot
                existing_worker = self._workers.get(worker_id)
                if existing_worker is None:
                    self._workers[worker_id] = WorkerNodeRecord(
                        worker_id=worker_id,
                        slot_count=slot_index + 1,
                        last_heartbeat_us=0,
                        source="external",
                        status="online",
                        session_id=None,
                    )
                else:
                    existing_worker.slot_count = max(existing_worker.slot_count, slot_index + 1)
            if slot is None:
                continue
            access_mode = str(slot_payload.get("access_mode") or "shared").strip() or "shared"
            if access_mode not in {"shared", "dedicated"}:
                access_mode = "shared"
            dedicated_username = normalize_principal_id(
                slot_payload.get("dedicated_username")
            )
            owner_principal_id = normalize_principal_id(
                slot_payload.get("owner_principal_id")
            )
            slot.access_mode = access_mode
            slot.dedicated_username = (
                dedicated_username or owner_principal_id
                if access_mode == "dedicated"
                else None
            )

        for job_payload in payload.get("jobs", []):
            job = self._job_from_persisted_payload(job_payload)
            if job is None:
                continue
            self._jobs[job.job_id] = job
            state_changed_on_load = (
                state_changed_on_load
                or self._job_differs_from_persisted_payload(job, job_payload)
            )
            request_payload = job_payload.get("request")
            if isinstance(request_payload, dict):
                self._job_requests[job.job_id] = request_payload
            self._job_sequence = max(self._job_sequence, job.sequence)
        state_changed_on_load = (
            self._restore_queued_job_runtime() or state_changed_on_load
        )
        state_changed_on_load = (
            self._restore_recovered_slot_activity() or state_changed_on_load
        )
        if state_changed_on_load:
            self._write_persist_payload(self._build_persist_payload())
        if state_source is not None:
            LOG.info("loaded scheduler state from %s", state_source)

    def _job_from_persisted_payload(
        self,
        job_payload: dict[str, Any],
        *,
        recover_inflight: bool = True,
    ) -> JobRecord | None:
        job_id = str(job_payload.get("job_id") or "")
        if not job_id:
            return None
        worker_id = str(job_payload.get("worker_id") or self._worker_id)
        status = str(job_payload.get("status") or "failed")
        is_external_recovery = (
            recover_inflight
            and worker_id != self._worker_id
            and status in RUNNING_JOB_STATUSES
        )
        if status == "queued":
            message = str(job_payload.get("message") or "Recovered queued job")
            finished_at_us = None
        elif is_external_recovery:
            if status == "cancelling" or bool(job_payload.get("stop_requested", False)):
                message = (
                    "Recovered after control-plane restart; stop was already "
                    "requested and the control plane is awaiting the external "
                    "worker's terminal update"
                )
                status = "cancelling"
            else:
                message = (
                    "Recovered after control-plane restart; awaiting external "
                    "worker state"
                )
                status = "running"
            finished_at_us = None
        elif recover_inflight and status not in TERMINAL_JOB_STATUSES:
            status = "failed"
            message = (
                "Recovered after control-plane restart; previous in-flight state was lost"
            )
            finished_at_us = now_us()
        else:
            message = str(job_payload.get("message") or "")
            finished_at_us = (
                None
                if status in RUNNING_JOB_STATUSES and not recover_inflight
                else job_payload.get("finished_at_us")
            )
        return JobRecord(
            job_id=job_id,
            job_type=str(job_payload.get("job_type") or "train_validate"),
            owner_principal_id=job_payload.get("owner_principal_id"),
            worker_id=worker_id,
            thread_slot_id=str(job_payload.get("thread_slot_id") or ""),
            priority=int(job_payload.get("priority") or 0),
            status=status,
            message=message,
            report=job_payload.get("report"),
            error=job_payload.get("error"),
            traceback_text=job_payload.get("traceback_text"),
            stop_requested=bool(job_payload.get("stop_requested", False)),
            created_at_us=int(job_payload.get("created_at_us") or 0),
            started_at_us=job_payload.get("started_at_us"),
            finished_at_us=finished_at_us,
            sequence=int(job_payload.get("sequence") or 0),
            queue_token=int(job_payload.get("queue_token") or 0),
            restart_count=int(job_payload.get("restart_count") or 0),
        )

    def _merge_external_state_from_payload(self, payload: dict[str, Any]) -> None:
        for job_id, job in list(self._jobs.items()):
            if job.worker_id == self._worker_id:
                continue
            self._jobs.pop(job_id, None)
            self._job_requests.pop(job_id, None)
            self._job_payloads.pop(job_id, None)
            self._job_stop_events.pop(job_id, None)
        for slot_id, slot in list(self._slots.items()):
            if slot.worker_id != self._worker_id:
                self._slots.pop(slot_id, None)
        for worker_id in list(self._workers.keys()):
            if worker_id != self._worker_id:
                self._workers.pop(worker_id, None)

        external_slots: dict[str, ThreadSlotState] = {}
        for worker_payload in payload.get("workers", []):
            worker_id = str(worker_payload.get("worker_id") or "").strip()
            if not worker_id or worker_id == self._worker_id:
                continue
            slot_count = int(worker_payload.get("slot_count") or 0)
            if slot_count <= 0:
                continue
            self._workers[worker_id] = WorkerNodeRecord(
                worker_id=worker_id,
                slot_count=slot_count,
                last_heartbeat_us=int(worker_payload.get("last_heartbeat_us") or 0),
                source=str(worker_payload.get("source") or "external"),
                status=str(worker_payload.get("status") or "online"),
                session_id=normalize_principal_id(worker_payload.get("session_id")),
            )

        for slot_payload in payload.get("slots", []):
            slot_id = str(slot_payload.get("slot_id") or "")
            worker_id = str(slot_payload.get("worker_id") or self._worker_id)
            if not slot_id or worker_id == self._worker_id:
                continue
            slot_index = int(slot_payload.get("slot_index") or 0)
            slot = ThreadSlotState(
                slot_id=slot_id,
                worker_id=worker_id,
                slot_index=slot_index,
                access_mode=str(slot_payload.get("access_mode") or "shared"),
                dedicated_username=normalize_principal_id(
                    slot_payload.get("dedicated_username")
                    or slot_payload.get("owner_principal_id")
                ),
            )
            self._slots[slot_id] = slot
            external_slots[slot_id] = slot
            existing_worker = self._workers.get(worker_id)
            if existing_worker is None:
                self._workers[worker_id] = WorkerNodeRecord(
                    worker_id=worker_id,
                    slot_count=slot_index + 1,
                    last_heartbeat_us=0,
                    source="external",
                    status="online",
                    session_id=None,
                )
            else:
                existing_worker.slot_count = max(existing_worker.slot_count, slot_index + 1)

        external_jobs: list[JobRecord] = []
        for job_payload in payload.get("jobs", []):
            worker_id = str(job_payload.get("worker_id") or self._worker_id)
            if worker_id == self._worker_id:
                continue
            job = self._job_from_persisted_payload(
                job_payload,
                recover_inflight=False,
            )
            if job is None:
                continue
            self._jobs[job.job_id] = job
            request_payload = job_payload.get("request")
            if isinstance(request_payload, dict):
                self._job_requests[job.job_id] = request_payload
            self._job_sequence = max(self._job_sequence, job.sequence)
            external_jobs.append(job)

        for slot in external_slots.values():
            slot.current_job_id = None
            slot.busy_intervals.clear()

        for job in sorted(
            [item for item in external_jobs if item.status in RUNNING_JOB_STATUSES],
            key=lambda item: (
                item.thread_slot_id,
                int(item.started_at_us or 0),
                item.sequence,
            ),
        ):
            slot = external_slots.get(job.thread_slot_id)
            if slot is None:
                continue
            if slot.current_job_id is not None and slot.current_job_id != job.job_id:
                LOG.warning(
                    "conflicting running jobs for external slot %s while refreshing shared state",
                    slot.slot_id,
                )
                continue
            slot.current_job_id = job.job_id
            slot.busy_intervals.append((time.monotonic(), None))

        for job in sorted(
            [item for item in external_jobs if item.status == "queued"],
            key=lambda item: (item.sequence, item.created_at_us),
        ):
            slot = external_slots.get(job.thread_slot_id)
            if slot is None:
                continue
            slot.queue.put_nowait(
                (-job.priority, job.sequence, job.job_id, job.queue_token)
            )

    async def _refresh_shared_state_from_db_if_needed(
        self,
        *,
        force: bool = False,
    ) -> None:
        policies = await asyncio.to_thread(self._auth_store.list_compute_slot_policies)
        self._apply_compute_slot_policies(policies)
        if self._state_db_path is None:
            return
        now_monotonic = time.monotonic()
        if (
            not force
            and self._last_state_db_refresh_check_monotonic > 0.0
            and now_monotonic - self._last_state_db_refresh_check_monotonic
            < SHARED_STATE_DB_POLL_INTERVAL_S
        ):
            return
        self._last_state_db_refresh_check_monotonic = now_monotonic
        revision = await asyncio.to_thread(
            load_scheduler_state_db_revision, self._state_db_path
        )
        if revision <= self._state_db_revision:
            return
        payload = await asyncio.to_thread(load_scheduler_state_db, self._state_db_path)
        async with self._jobs_lock:
            self._merge_external_state_from_payload(payload)
            self._state_db_revision = max(revision, scheduler_state_revision(payload))
        self._last_state_db_refresh_check_monotonic = time.monotonic()

    def _restore_recovered_slot_activity(self) -> bool:
        changed = False
        recovered_jobs = sorted(
            [
                job
                for job in self._jobs.values()
                if job.worker_id != self._worker_id
                and job.status in RUNNING_JOB_STATUSES
            ],
            key=lambda item: (
                item.thread_slot_id,
                int(item.started_at_us or 0),
                item.sequence,
            ),
        )
        for job in recovered_jobs:
            slot = self._slots.get(job.thread_slot_id)
            if slot is None:
                job.status = "failed"
                job.message = (
                    "Recovered after control-plane restart but the slot "
                    "inventory for this job is no longer present"
                )
                job.error = "recovered slot inventory missing"
                job.finished_at_us = now_us()
                changed = True
                continue
            if slot.current_job_id is not None and slot.current_job_id != job.job_id:
                job.status = "failed"
                job.message = (
                    "Recovered after control-plane restart with conflicting "
                    "running state on the same slot"
                )
                job.error = "conflicting recovered running slot state"
                job.finished_at_us = now_us()
                changed = True
                continue
            slot.current_job_id = job.job_id
            if not slot.busy_intervals or slot.busy_intervals[-1][1] is not None:
                slot.busy_intervals.append((time.monotonic(), None))
        return changed

    def _restore_queued_job_runtime(self) -> bool:
        changed = False
        queued_jobs = sorted(
            [job for job in self._jobs.values() if job.status == "queued"],
            key=lambda item: (item.sequence, item.created_at_us),
        )
        for job in queued_jobs:
            slot = self._slots.get(job.thread_slot_id)
            if slot is None:
                job.status = "failed"
                job.message = (
                    "Recovered after control-plane restart but the slot "
                    "inventory for this queued job is no longer present"
                )
                job.error = "recovered queued slot inventory missing"
                job.finished_at_us = now_us()
                changed = True
                continue
            if not isinstance(self._job_requests.get(job.job_id), dict):
                job.status = "failed"
                job.message = (
                    "Recovered after control-plane restart but the queued job "
                    "request payload is missing"
                )
                job.error = "recovered queued request payload missing"
                job.finished_at_us = now_us()
                slot.failed_jobs += 1
                self._job_requests.pop(job.job_id, None)
                changed = True
                continue
            slot.queue.put_nowait(
                (-job.priority, job.sequence, job.job_id, job.queue_token)
            )
            if job.worker_id == self._worker_id:
                self._job_stop_events.setdefault(job.job_id, threading.Event())
        return changed

    async def _require_matching_worker_session(
        self,
        websocket: ServerConnection,
        payload: dict[str, Any],
        *,
        worker_id: str,
        request_id: Any,
    ) -> WorkerNodeRecord | None:
        worker = self._workers.get(worker_id)
        if worker is None or worker_id == self._worker_id:
            return worker
        expected_session_id = normalize_principal_id(worker.session_id)
        if expected_session_id is None:
            return worker
        provided_session_id = normalize_principal_id(payload.get("worker_session_id"))
        if provided_session_id is None:
            await self._send_error(
                websocket,
                f"worker_session_id is required for worker {worker_id}",
                request_id=request_id,
            )
            return None
        if provided_session_id != expected_session_id:
            await self._send_error(
                websocket,
                f"worker {worker_id} session mismatch",
                request_id=request_id,
            )
            return None
        return worker

    async def _restart_jobs_for_restarted_worker(self, worker_id: str) -> list[JobRecord]:
        changed_jobs: list[JobRecord] = []
        async with self._jobs_lock:
            active_jobs = sorted(
                [
                    job
                    for job in self._jobs.values()
                    if job.worker_id == worker_id and job.status in RUNNING_JOB_STATUSES
                ],
                key=lambda item: (item.thread_slot_id, item.sequence, item.created_at_us),
            )
            for job in active_jobs:
                slot = self._slots.get(job.thread_slot_id)
                if slot is not None and slot.current_job_id == job.job_id:
                    if slot.busy_intervals:
                        start, end = slot.busy_intervals[-1]
                        if end is None:
                            slot.busy_intervals[-1] = (start, time.monotonic())
                    slot.current_job_id = None
                stop_event = self._job_stop_events.pop(job.job_id, None)
                if stop_event is not None:
                    stop_event.set()

                if job.stop_requested or job.status == "cancelling":
                    job.status = "cancelled"
                    job.message = (
                        "Cancelled after worker process restart because stop was already requested"
                    )
                    job.finished_at_us = now_us()
                    if slot is not None:
                        slot.cancelled_jobs += 1
                    self._job_payloads.pop(job.job_id, None)
                    self._job_requests.pop(job.job_id, None)
                    changed_jobs.append(job)
                    continue

                request_payload = self._job_requests.get(job.job_id)
                if not isinstance(request_payload, dict):
                    job.status = "failed"
                    job.message = (
                        "Worker process restarted but the scheduler request payload for this job is missing"
                    )
                    job.error = "scheduler request payload missing after worker restart"
                    job.finished_at_us = now_us()
                    if slot is not None:
                        slot.failed_jobs += 1
                    self._job_payloads.pop(job.job_id, None)
                    self._job_requests.pop(job.job_id, None)
                    changed_jobs.append(job)
                    continue

                if job.restart_count >= self._max_worker_restart_attempts:
                    job.status = "failed"
                    job.message = (
                        "Worker process restarted and automatic rerun limit was exhausted; "
                        "the job must be resubmitted"
                    )
                    job.error = (
                        "worker process restart rerun limit exhausted"
                    )
                    job.finished_at_us = now_us()
                    if slot is not None:
                        slot.failed_jobs += 1
                    self._job_payloads.pop(job.job_id, None)
                    self._job_requests.pop(job.job_id, None)
                    changed_jobs.append(job)
                    continue

                job.status = "queued"
                job.message = "Requeued after worker process restart"
                job.error = None
                job.traceback_text = None
                job.report = None
                job.started_at_us = None
                job.finished_at_us = None
                job.queue_token += 1
                job.restart_count += 1
                if slot is None:
                    job.status = "failed"
                    job.message = (
                        "Worker process restarted but the slot inventory for this job is no longer present"
                    )
                    job.error = "slot inventory missing after worker restart"
                    job.finished_at_us = now_us()
                    changed_jobs.append(job)
                    continue
                slot.queue.put_nowait(
                    (-job.priority, job.sequence, job.job_id, job.queue_token)
                )
                changed_jobs.append(job)
        return changed_jobs

    async def _persist_state(self) -> None:
        if self._state_path is None and self._state_db_path is None:
            return
        async with self._jobs_lock:
            payload = self._build_persist_payload()
        self._write_persist_payload(payload)

    async def handle_client(self, websocket: ServerConnection) -> None:
        self._clients.add(websocket)
        authenticated_user = self._auth_store.authenticate_cookie_header(
            websocket_cookie_header(websocket)
        )
        self._client_users[websocket] = authenticated_user
        scoped_principal = (
            None
            if authenticated_user is None or authenticated_user.is_admin
            else authenticated_user.username
        )
        try:
            await self._refresh_shared_state_from_db_if_needed(force=True)
            await websocket.send(
                json.dumps(
                    {
                        "type": "hello",
                        "service": "natkit-ml-control-plane",
                        "broker": self._base_args.broker,
                        "worker_id": self._worker_id,
                        "slot_count": self._slot_count,
                        "default_principal_id": self._default_principal_id,
                        "authenticated_user": self._authenticated_user_payload(
                            authenticated_user
                        ),
                    }
                )
            )
            await self._send_workers(
                websocket, request_id=None, principal_id=scoped_principal, user=authenticated_user
            )
            await self._send_thread_slots(
                websocket, request_id=None, principal_id=scoped_principal, user=authenticated_user
            )
            await self._send_job_list(
                websocket, request_id=None, principal_id=scoped_principal
            )
            async for raw in websocket:
                await self._handle_message(websocket, raw)
        finally:
            self._client_users.pop(websocket, None)
            self._clients.discard(websocket)

    async def _handle_message(self, websocket: ServerConnection, raw: str) -> None:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            await self._send_error(websocket, f"invalid JSON: {exc}")
            return

        await self._refresh_shared_state_from_db_if_needed()
        action = str(payload.get("action") or "")
        request_id = payload.get("request_id")
        if action:
            user = await self._require_authenticated_user(websocket, request_id)
            if user is None:
                return
        if action == "list_recorded_runs":
            await self._handle_list_recorded_runs(websocket, payload, request_id)
            return
        if action == "list_workers":
            await self._send_workers(
                websocket,
                request_id=request_id,
                user=self._connection_user(websocket),
                show_all=bool(payload.get("show_all")),
            )
            return
        if action == "list_thread_slots":
            await self._send_thread_slots(
                websocket,
                request_id=request_id,
                user=self._connection_user(websocket),
                show_all=bool(payload.get("show_all")),
            )
            return
        if action == "set_slot_compute_policy":
            await self._handle_set_slot_compute_policy(websocket, payload, request_id)
            return
        if action == "assign_thread_slots":
            await self._handle_assign_thread_slots(websocket, payload, request_id)
            return
        if action == "release_thread_slots":
            await self._handle_release_thread_slots(websocket, payload, request_id)
            return
        if action == "update_worker_status":
            await self._handle_update_worker_status(websocket, payload, request_id)
            return
        if action == "wait_worker_drain_ready":
            await self._handle_wait_worker_drain_ready(websocket, payload, request_id)
            return
        if action == "register_worker":
            await self._handle_register_worker(websocket, payload, request_id)
            return
        if action == "worker_heartbeat":
            await self._handle_worker_heartbeat(websocket, payload, request_id)
            return
        if action == "sync_worker_slots":
            await self._handle_sync_worker_slots(websocket, payload, request_id)
            return
        if action == "unregister_worker":
            await self._handle_unregister_worker(websocket, payload, request_id)
            return
        if action == "claim_worker_slot_job":
            await self._handle_claim_worker_slot_job(websocket, payload, request_id)
            return
        if action == "report_worker_job_state":
            await self._handle_report_worker_job_state(websocket, payload, request_id)
            return
        if action == "list_jobs":
            await self._send_job_list(
                websocket,
                request_id=request_id,
                principal_id=self._resolve_scheduler_principal(
                    websocket, payload, allow_all_for_admin=True
                ),
                thread_slot_id=payload.get("thread_slot_id"),
            )
            return
        if action == "start_train_validate_job":
            await self._handle_start_train_validate_job(websocket, payload, request_id)
            return
        if action == "get_job_status":
            await self._handle_get_job_status(websocket, payload, request_id)
            return
        if action == "update_job":
            await self._handle_update_job(websocket, payload, request_id)
            return
        if action == "stop_job":
            await self._handle_stop_job(websocket, payload, request_id)
            return
        if action == "recover_job":
            await self._handle_recover_job(websocket, payload, request_id)
            return
        if action == "recover_worker":
            await self._handle_recover_worker(websocket, payload, request_id)
            return
        await self._send_error(websocket, f"unknown action: {action}", request_id=request_id)

    async def _handle_list_recorded_runs(
        self,
        websocket: ServerConnection,
        payload: dict[str, Any],
        request_id: Any,
    ) -> None:
        if await self._require_authenticated_user(websocket, request_id) is None:
            return
        args = namespace_from_config(
            self._base_args,
            {
                "broker": payload.get("broker", self._base_args.broker),
            },
        )
        runs = await asyncio.to_thread(discover_runs, args)
        await websocket.send(
            json.dumps(
                {
                    "type": "recorded_runs",
                    "request_id": request_id,
                    "broker": args.broker,
                    "runs": [run_to_dict(run) for run in runs],
                }
            )
        )

    async def _handle_assign_thread_slots(
        self,
        websocket: ServerConnection,
        payload: dict[str, Any],
        request_id: Any,
    ) -> None:
        await self._send_error(
            websocket,
            "slot assignment is obsolete; admins now manage slot compute policy",
            request_id=request_id,
        )

    async def _handle_release_thread_slots(
        self,
        websocket: ServerConnection,
        payload: dict[str, Any],
        request_id: Any,
    ) -> None:
        await self._send_error(
            websocket,
            "slot release is obsolete; admins now manage slot compute policy",
            request_id=request_id,
        )

    async def _handle_set_slot_compute_policy(
        self,
        websocket: ServerConnection,
        payload: dict[str, Any],
        request_id: Any,
    ) -> None:
        user = await self._require_admin_user(websocket, request_id)
        if user is None:
            return
        slot_id = str(payload.get("slot_id") or "").strip()
        access_mode = str(payload.get("access_mode") or "").strip()
        dedicated_username = normalize_principal_id(payload.get("dedicated_username"))
        if not slot_id:
            await self._send_error(websocket, "slot_id is required", request_id=request_id)
            return
        slot = self._slots.get(slot_id)
        if slot is None:
            await self._send_error(websocket, f"slot_id not found: {slot_id}", request_id=request_id)
            return
        if access_mode not in {"shared", "dedicated"}:
            await self._send_error(
                websocket,
                "access_mode must be shared or dedicated",
                request_id=request_id,
            )
            return
        if access_mode == "dedicated" and not dedicated_username:
            await self._send_error(
                websocket,
                "dedicated_username is required for dedicated slots",
                request_id=request_id,
            )
            return

        active_jobs = self._active_jobs_for_slot(slot_id)
        if active_jobs:
            await self._send_error(
                websocket,
                f"slot {slot_id} has active jobs and cannot change policy",
                request_id=request_id,
            )
            return

        slot.access_mode = access_mode
        slot.dedicated_username = dedicated_username if access_mode == "dedicated" else None
        await asyncio.to_thread(
            self._auth_store.set_compute_slot_policy,
            slot_id=slot_id,
            access_mode=slot.access_mode,
            dedicated_username=slot.dedicated_username,
            updated_at_us=now_us(),
        )
        await self._send_thread_slots(
            websocket,
            request_id=request_id,
            user=self._connection_user(websocket),
            show_all=bool(payload.get("show_all")),
        )
        await self._persist_state()
        await self._broadcast_workers()
        await self._broadcast_thread_slots()

    async def _handle_update_worker_status(
        self,
        websocket: ServerConnection,
        payload: dict[str, Any],
        request_id: Any,
    ) -> None:
        user = self._connection_user(websocket)
        if user is not None and not user.is_admin:
            await self._send_error(
                websocket,
                "admin access required",
                request_id=request_id,
            )
            return
        worker_id = str(payload.get("worker_id") or "").strip()
        status = str(payload.get("status") or "").strip() or "online"
        if not worker_id:
            await self._send_error(
                websocket,
                "worker_id is required",
                request_id=request_id,
            )
            return
        if status not in CONTROLLED_WORKER_STATUSES:
            await self._send_error(
                websocket,
                (
                    "status must be one of "
                    + ", ".join(sorted(CONTROLLED_WORKER_STATUSES))
                ),
                request_id=request_id,
            )
            return
        worker = self._workers.get(worker_id)
        if worker is None:
            await self._send_error(
                websocket,
                f"worker_id not found: {worker_id}",
                request_id=request_id,
            )
            return

        self._workers[worker_id] = WorkerNodeRecord(
            worker_id=worker.worker_id,
            slot_count=worker.slot_count,
            last_heartbeat_us=worker.last_heartbeat_us,
            source=worker.source,
            status=status,
            session_id=worker.session_id,
        )
        await self._persist_state()
        await self._send_workers(websocket, request_id=request_id, principal_id=None)
        await self._broadcast_workers()
        await self._broadcast_thread_slots()

    async def _handle_wait_worker_drain_ready(
        self,
        websocket: ServerConnection,
        payload: dict[str, Any],
        request_id: Any,
    ) -> None:
        user = self._connection_user(websocket)
        if user is not None and not user.is_admin:
            await self._send_error(
                websocket,
                "admin access required",
                request_id=request_id,
            )
            return
        worker_id = str(payload.get("worker_id") or "").strip()
        timeout_s = float(payload.get("timeout_s", DEFAULT_DRAIN_WAIT_TIMEOUT_S))
        if not worker_id:
            await self._send_error(
                websocket,
                "worker_id is required",
                request_id=request_id,
            )
            return
        if timeout_s < 0:
            await self._send_error(
                websocket,
                "timeout_s must be greater than or equal to zero",
                request_id=request_id,
            )
            return
        if worker_id not in self._workers:
            await self._send_error(
                websocket,
                f"worker_id not found: {worker_id}",
                request_id=request_id,
            )
            return

        deadline = time.monotonic() + timeout_s
        timed_out = False
        while True:
            await self._refresh_shared_state_from_db_if_needed(force=True)
            worker_payload = await self._worker_summary_payload(
                worker_id,
                principal_id=None,
                user=self._connection_user(websocket),
                show_all=True,
            )
            if worker_payload is None:
                await self._send_error(
                    websocket,
                    f"worker_id not found: {worker_id}",
                    request_id=request_id,
                )
                return
            if bool(worker_payload.get("drain_ready")):
                await websocket.send(
                    json.dumps(
                        {
                            "type": "worker_drain_status",
                            "request_id": request_id,
                            "timed_out": False,
                            **worker_payload,
                        }
                    )
                )
                return
            remaining_s = deadline - time.monotonic()
            if remaining_s <= 0:
                timed_out = True
                break
            await asyncio.sleep(min(SHARED_STATE_DB_POLL_INTERVAL_S, remaining_s))

        worker_payload = await self._worker_summary_payload(
            worker_id,
            principal_id=None,
            user=self._connection_user(websocket),
            show_all=True,
        )
        if worker_payload is None:
            await self._send_error(
                websocket,
                f"worker_id not found: {worker_id}",
                request_id=request_id,
            )
            return
        await websocket.send(
            json.dumps(
                {
                    "type": "worker_drain_status",
                    "request_id": request_id,
                    "timed_out": timed_out,
                    **worker_payload,
                }
            )
        )

    async def _handle_start_train_validate_job(
        self,
        websocket: ServerConnection,
        payload: dict[str, Any],
        request_id: Any,
    ) -> None:
        user = await self._require_authenticated_user(websocket, request_id)
        if user is None:
            return
        thread_slot_id = str(payload.get("thread_slot_id") or "").strip()
        if not thread_slot_id:
            await self._send_error(
                websocket,
                "thread_slot_id is required",
                request_id=request_id,
            )
            return

        slot = self._slots.get(thread_slot_id)
        if slot is None:
            await self._send_error(
                websocket,
                f"thread_slot_id not found: {thread_slot_id}",
                request_id=request_id,
            )
            return

        principal_id = user.username
        if not self._slot_is_submittable_by_user(slot, user):
            slot_label = (
                f"dedicated to {slot.dedicated_username}"
                if slot.access_mode == "dedicated"
                else "shared-pool access denied"
            )
            await self._send_error(
                websocket,
                f"slot {thread_slot_id} is not available to {principal_id}: {slot_label}",
                request_id=request_id,
            )
            return
        if self._resolved_worker_status(slot.worker_id) == "draining":
            await self._send_error(
                websocket,
                (
                    f"slot {thread_slot_id} is attached to worker {slot.worker_id} "
                    "which is draining and not accepting new jobs"
                ),
                request_id=request_id,
            )
            return
        request_payload = build_job_request_payload(self._base_args, payload)
        try:
            parse_selected_channel_indexes(request_payload.get("selected_fields"))
        except ValueError as exc:
            await self._send_error(
                websocket,
                str(exc),
                request_id=request_id,
            )
            return
        if not request_payload["train_runs"] or not request_payload["eval_runs"]:
            await self._send_error(
                websocket,
                "train_runs and eval_runs are required",
                request_id=request_id,
            )
            return

        priority = int(payload.get("priority", 0))
        self._job_sequence += 1
        job_id = str(uuid.uuid4())
        is_local_worker = slot.worker_id == self._worker_id
        job = JobRecord(
            job_id=job_id,
            owner_principal_id=principal_id,
            worker_id=slot.worker_id,
            thread_slot_id=thread_slot_id,
            priority=priority,
            status="queued",
            message=(
                f"Queued on {thread_slot_id}"
                if is_local_worker
                else f"Queued for external worker claim on {thread_slot_id}"
            ),
            created_at_us=now_us(),
            sequence=self._job_sequence,
            queue_token=0,
        )
        async with self._jobs_lock:
            self._jobs[job_id] = job

        await slot.queue.put((-priority, job.sequence, job_id, job.queue_token))
        self._job_requests[job_id] = request_payload
        if is_local_worker:
            job_workspace = create_job_workspace(self._base_args)
            pipeline_args = build_pipeline_namespace(
                self._base_args,
                request_payload,
                output_dir=str(job_workspace),
            )
            self._job_payloads[job_id] = (pipeline_args, job_workspace)
            self._job_stop_events[job_id] = threading.Event()

        await websocket.send(
            json.dumps(
                {
                    "type": "job_accepted",
                    "request_id": request_id,
                    "job_id": job_id,
                    "status": job.status,
                    "message": job.message,
                    "thread_slot_id": thread_slot_id,
                    "priority": priority,
                }
            )
        )
        await self._persist_state()
        self._touch_worker_heartbeat()
        await self._broadcast_workers()
        await self._broadcast_thread_slots()
        await self._broadcast_job_status(job)
        await self._broadcast_job_list()

    async def _handle_register_worker(
        self,
        websocket: ServerConnection,
        payload: dict[str, Any],
        request_id: Any,
    ) -> None:
        worker_id = str(payload.get("worker_id") or "").strip()
        slot_count = int(payload.get("slot_count") or 0)
        if not worker_id:
            await self._send_error(
                websocket,
                "worker_id is required",
                request_id=request_id,
            )
            return
        if slot_count <= 0:
            await self._send_error(
                websocket,
                "slot_count must be greater than zero",
                request_id=request_id,
            )
            return
        if worker_id == self._worker_id:
            await self._send_error(
                websocket,
                "embedded worker is managed by the control plane",
                request_id=request_id,
            )
            return

        existing = self._workers.get(worker_id)
        status = merge_worker_status(
            existing.status if existing is not None else None,
            payload.get("status"),
        )
        session_id = normalize_principal_id(payload.get("worker_session_id"))
        restarted_jobs: list[JobRecord] = []
        if (
            existing is not None
            and existing.session_id is not None
            and session_id is not None
            and existing.session_id != session_id
        ):
            restarted_jobs = await self._restart_jobs_for_restarted_worker(worker_id)
        self._workers[worker_id] = WorkerNodeRecord(
            worker_id=worker_id,
            slot_count=slot_count,
            last_heartbeat_us=now_us(),
            source="external",
            status=status,
            session_id=session_id,
        )
        await self._persist_state()
        await self._send_workers(websocket, request_id=request_id, principal_id=None)
        await self._broadcast_workers()
        if restarted_jobs:
            await self._broadcast_thread_slots()
            for job in restarted_jobs:
                await self._broadcast_job_status(job)
            await self._broadcast_job_list()

    async def _handle_worker_heartbeat(
        self,
        websocket: ServerConnection,
        payload: dict[str, Any],
        request_id: Any,
    ) -> None:
        worker_id = str(payload.get("worker_id") or "").strip()
        if not worker_id:
            await self._send_error(
                websocket,
                "worker_id is required",
                request_id=request_id,
            )
            return
        if worker_id == self._worker_id:
            self._touch_worker_heartbeat()
            await self._send_workers(websocket, request_id=request_id, principal_id=None)
            await self._broadcast_workers()
            return

        slot_count = int(payload.get("slot_count") or 0)
        existing = self._workers.get(worker_id)
        resolved_slot_count = slot_count
        if resolved_slot_count <= 0 and existing is not None:
            resolved_slot_count = existing.slot_count
        if resolved_slot_count <= 0:
            await self._send_error(
                websocket,
                "slot_count must be provided for unknown workers",
                request_id=request_id,
            )
            return

        status = merge_worker_status(
            existing.status if existing is not None else None,
            payload.get("status")
            or (existing.status if existing is not None else "online"),
        )
        session_id = normalize_principal_id(payload.get("worker_session_id"))
        if (
            existing is not None
            and existing.session_id is not None
            and session_id != existing.session_id
        ):
            await self._send_error(
                websocket,
                f"worker {worker_id} session mismatch",
                request_id=request_id,
            )
            return
        self._workers[worker_id] = WorkerNodeRecord(
            worker_id=worker_id,
            slot_count=resolved_slot_count,
            last_heartbeat_us=now_us(),
            source="external",
            status=status,
            session_id=session_id or (existing.session_id if existing is not None else None),
        )
        await self._persist_state()
        await self._send_workers(websocket, request_id=request_id, principal_id=None)
        await self._broadcast_workers()

    async def _handle_unregister_worker(
        self,
        websocket: ServerConnection,
        payload: dict[str, Any],
        request_id: Any,
    ) -> None:
        worker_id = str(payload.get("worker_id") or "").strip()
        if not worker_id:
            await self._send_error(
                websocket,
                "worker_id is required",
                request_id=request_id,
            )
            return
        if worker_id == self._worker_id:
            await self._send_error(
                websocket,
                "embedded worker cannot be unregistered",
                request_id=request_id,
            )
            return
        if worker_id not in self._workers:
            await self._send_error(
                websocket,
                f"worker_id not found: {worker_id}",
                request_id=request_id,
            )
            return
        if (
            await self._require_matching_worker_session(
                websocket,
                payload,
                worker_id=worker_id,
                request_id=request_id,
            )
            is None
        ):
            return
        worker_slots = [
            slot.slot_id for slot in self._slots.values() if slot.worker_id == worker_id
        ]
        for slot_id in worker_slots:
            if self._active_jobs_for_slot(slot_id):
                await self._send_error(
                    websocket,
                    (
                        f"worker {worker_id} still has active jobs attached; "
                        "release inventory after work is drained"
                    ),
                    request_id=request_id,
                )
                return
        self._workers.pop(worker_id, None)
        for slot_id in worker_slots:
            self._slots.pop(slot_id, None)
        await self._persist_state()
        await self._send_workers(websocket, request_id=request_id, principal_id=None)
        await self._broadcast_workers()
        await self._broadcast_thread_slots()

    async def _handle_sync_worker_slots(
        self,
        websocket: ServerConnection,
        payload: dict[str, Any],
        request_id: Any,
    ) -> None:
        worker_id = str(payload.get("worker_id") or "").strip()
        slot_count = int(payload.get("slot_count") or 0)
        if not worker_id:
            await self._send_error(
                websocket,
                "worker_id is required",
                request_id=request_id,
            )
            return
        if worker_id == self._worker_id:
            await self._send_error(
                websocket,
                "embedded worker inventory is managed locally",
                request_id=request_id,
            )
            return
        if slot_count <= 0:
            await self._send_error(
                websocket,
                "slot_count must be greater than zero",
                request_id=request_id,
            )
            return
        worker = self._workers.get(worker_id)
        if worker is None:
            await self._send_error(
                websocket,
                f"worker_id not found: {worker_id}",
                request_id=request_id,
            )
            return
        validated_worker = await self._require_matching_worker_session(
            websocket,
            payload,
            worker_id=worker_id,
            request_id=request_id,
        )
        if validated_worker is None:
            return

        default_owner_principal_id = normalize_principal_id(
            payload.get("default_owner_principal_id")
        )
        existing_slots = sorted(
            [slot for slot in self._slots.values() if slot.worker_id == worker_id],
            key=lambda item: item.slot_index,
        )
        removable_slots = [slot for slot in existing_slots if slot.slot_index >= slot_count]
        for slot in removable_slots:
            if self._active_jobs_for_slot(slot.slot_id):
                await self._send_error(
                    websocket,
                    (
                        f"worker {worker_id} cannot shrink inventory while "
                        f"{slot.slot_id} still has active jobs"
                    ),
                    request_id=request_id,
                )
                return
            if slot.access_mode == "dedicated" and slot.dedicated_username is not None:
                await self._send_error(
                    websocket,
                    (
                        f"worker {worker_id} cannot shrink inventory while "
                        f"{slot.slot_id} is still dedicated to {slot.dedicated_username}"
                    ),
                    request_id=request_id,
                )
                return

        for slot_index in range(slot_count):
            slot_id = build_thread_slot_id(worker_id, slot_index)
            if slot_id in self._slots:
                continue
            self._slots[slot_id] = ThreadSlotState(
                slot_id=slot_id,
                worker_id=worker_id,
                slot_index=slot_index,
                access_mode="shared",
                dedicated_username=None,
            )

        for slot in removable_slots:
            self._slots.pop(slot.slot_id, None)

        self._touch_worker_heartbeat(
            worker_id,
            slot_count=slot_count,
            source="external",
            status=validated_worker.status,
        )
        await self._persist_state()
        await self._send_thread_slots(
            websocket,
            request_id=request_id,
            principal_id=None,
        )
        await self._broadcast_workers()
        await self._broadcast_thread_slots()

    async def _handle_claim_worker_slot_job(
        self,
        websocket: ServerConnection,
        payload: dict[str, Any],
        request_id: Any,
    ) -> None:
        worker_id = str(payload.get("worker_id") or "").strip()
        slot_id = str(payload.get("slot_id") or "").strip()
        if not worker_id:
            await self._send_error(
                websocket,
                "worker_id is required",
                request_id=request_id,
            )
            return
        if not slot_id:
            await self._send_error(
                websocket,
                "slot_id is required",
                request_id=request_id,
            )
            return
        slot = self._slots.get(slot_id)
        if slot is None:
            await self._send_error(
                websocket,
                f"slot_id not found: {slot_id}",
                request_id=request_id,
            )
            return
        if slot.worker_id != worker_id:
            await self._send_error(
                websocket,
                f"slot {slot_id} is owned by worker {slot.worker_id}",
                request_id=request_id,
            )
            return
        if (
            await self._require_matching_worker_session(
                websocket,
                payload,
                worker_id=worker_id,
                request_id=request_id,
            )
            is None
        ):
            return
        if worker_id == self._worker_id:
            await self._send_error(
                websocket,
                "embedded worker jobs are claimed internally",
                request_id=request_id,
            )
            return
        if self._runtime_worker_status(worker_id) == "draining":
            self._touch_worker_heartbeat(
                worker_id,
                source="external",
                status="draining",
            )
            await websocket.send(
                json.dumps(
                    {
                        "type": "worker_job_claim",
                        "request_id": request_id,
                        "worker_id": worker_id,
                        "slot_id": slot_id,
                        "job": None,
                    }
                )
            )
            await self._broadcast_workers()
            return
        if slot.current_job_id is not None:
            await websocket.send(
                json.dumps(
                    {
                        "type": "worker_job_claim",
                        "request_id": request_id,
                        "worker_id": worker_id,
                        "slot_id": slot_id,
                        "job": None,
                    }
                )
            )
            return

        claimed_job: JobRecord | None = None
        request_payload: dict[str, Any] | None = None
        while not slot.queue.empty():
            _priority_key, _sequence, job_id, queue_token = await slot.queue.get()
            async with self._jobs_lock:
                job = self._jobs.get(job_id)
                if job is None or job.queue_token != queue_token or job.status != "queued":
                    continue
                request_payload = self._job_requests.get(job_id)
                if request_payload is None:
                    job.status = "failed"
                    job.message = "Scheduler request payload missing"
                    job.error = "scheduler request payload missing"
                    job.finished_at_us = now_us()
                    slot.failed_jobs += 1
                    claimed_job = job
                    break
                job.status = "running"
                job.message = f"Claimed by external worker {worker_id}"
                job.started_at_us = now_us()
                slot.current_job_id = job_id
                slot.busy_intervals.append((time.monotonic(), None))
                claimed_job = job
                break

        self._touch_worker_heartbeat(
            worker_id,
            source="external",
            status=self._runtime_worker_status(worker_id),
        )
        if claimed_job is None:
            await websocket.send(
                json.dumps(
                    {
                        "type": "worker_job_claim",
                        "request_id": request_id,
                        "worker_id": worker_id,
                        "slot_id": slot_id,
                        "job": None,
                    }
                )
            )
            await self._broadcast_workers()
            return

        await self._persist_state()
        await self._broadcast_workers()
        await self._broadcast_thread_slots()
        await self._broadcast_job_status(claimed_job)
        await self._broadcast_job_list()
        if request_payload is None:
            await websocket.send(
                json.dumps(
                    {
                        "type": "worker_job_claim",
                        "request_id": request_id,
                        "worker_id": worker_id,
                        "slot_id": slot_id,
                        "job": None,
                    }
                )
            )
            return
        await websocket.send(
            json.dumps(
                {
                    "type": "worker_job_claim",
                    "request_id": request_id,
                    "worker_id": worker_id,
                    "slot_id": slot_id,
                    "job": {
                        **job_to_dict(claimed_job),
                        "request": request_payload,
                    },
                }
            )
        )

    async def _handle_report_worker_job_state(
        self,
        websocket: ServerConnection,
        payload: dict[str, Any],
        request_id: Any,
    ) -> None:
        worker_id = str(payload.get("worker_id") or "").strip()
        job_id = str(payload.get("job_id") or "").strip()
        status = str(payload.get("status") or "").strip()
        message = str(payload.get("message") or "").strip()
        if not worker_id:
            await self._send_error(
                websocket,
                "worker_id is required",
                request_id=request_id,
            )
            return
        if not job_id:
            await self._send_error(
                websocket,
                "job_id is required",
                request_id=request_id,
            )
            return
        if status not in {"running", "completed", "failed", "cancelled"}:
            await self._send_error(
                websocket,
                f"unsupported worker job status: {status}",
                request_id=request_id,
            )
            return
        if (
            await self._require_matching_worker_session(
                websocket,
                payload,
                worker_id=worker_id,
                request_id=request_id,
            )
            is None
        ):
            return

        async with self._jobs_lock:
            job = self._jobs.get(job_id)
            if job is None:
                await self._send_error(
                    websocket,
                    f"job_id not found: {job_id}",
                    request_id=request_id,
                )
                return
            slot = self._slots.get(job.thread_slot_id)
            if slot is None:
                await self._send_error(
                    websocket,
                    f"thread_slot_id not found: {job.thread_slot_id}",
                    request_id=request_id,
                )
                return
            if slot.worker_id != worker_id:
                await self._send_error(
                    websocket,
                    f"job {job_id} is owned by worker {slot.worker_id}",
                    request_id=request_id,
                )
                return
            if worker_id == self._worker_id:
                await self._send_error(
                    websocket,
                    "embedded worker state is managed internally",
                    request_id=request_id,
                )
                return

            if status == "running":
                if job.status in TERMINAL_JOB_STATUSES:
                    await self._send_error(
                        websocket,
                        f"job {job_id} is already terminal in status {job.status}",
                        request_id=request_id,
                    )
                    return
                if job.started_at_us is None:
                    job.started_at_us = now_us()
                if slot.current_job_id is None:
                    slot.current_job_id = job_id
                    slot.busy_intervals.append((time.monotonic(), None))
                if job.stop_requested:
                    job.status = "cancelling"
                    job.message = message or "Stop requested; external worker is still finishing"
                else:
                    job.status = "running"
                    job.message = message or "External worker reported progress"
            else:
                if job.status in TERMINAL_JOB_STATUSES:
                    await self._send_error(
                        websocket,
                        f"job {job_id} is already terminal in status {job.status}",
                        request_id=request_id,
                    )
                    return
                if status == "completed":
                    job.status = "completed"
                    job.message = (
                        message
                        or (
                            "Training completed after stop was requested; immediate interruption is not yet supported for running jobs"
                            if job.stop_requested
                            else "Training and validation completed"
                        )
                    )
                    if payload.get("report") is not None:
                        # The worker already persisted artifacts to the shared
                        # /models and sanitized the report; preserve its durable
                        # model_path / bundle_path (re-sanitizing without them
                        # would strip the paths back to ephemeral).
                        report_in = payload.get("report") or {}
                        job.report = sanitize_pipeline_report(
                            report_in,
                            report_in.get("model_path"),
                            report_in.get("bundle_path"),
                        )
                    slot.completed_jobs += 1
                elif status == "failed":
                    job.status = "failed"
                    job.message = message or "External worker reported failure"
                    job.error = str(payload.get("error") or job.message)
                    job.traceback_text = payload.get("traceback_text")
                    slot.failed_jobs += 1
                else:
                    job.status = "cancelled"
                    job.message = message or "External worker reported cancellation"
                    slot.cancelled_jobs += 1
                job.finished_at_us = now_us()

        if status in TERMINAL_JOB_STATUSES:
            await self._finish_slot_run(slot)
            self._job_payloads.pop(job_id, None)
            self._job_requests.pop(job_id, None)
            self._job_stop_events.pop(job_id, None)

        self._touch_worker_heartbeat(
            worker_id,
            source="external",
            status=self._runtime_worker_status(worker_id),
        )
        await self._persist_state()
        await self._broadcast_workers()
        await self._broadcast_thread_slots()
        await self._broadcast_job_status(job)
        await self._broadcast_job_list()
        await websocket.send(
            json.dumps(
                {
                    "type": "job_status",
                    "request_id": request_id,
                    **job_to_dict(job),
                }
            )
        )

    async def _run_job_in_slot(
        self,
        slot: ThreadSlotState,
        job_id: str,
        pipeline_args: argparse.Namespace,
        job_workspace: Path,
    ) -> None:
        loop = asyncio.get_running_loop()

        def emit_progress(message: str) -> None:
            loop.call_soon_threadsafe(
                asyncio.create_task,
                self._set_job_state(
                    job_id,
                    status="running",
                    message=message,
                ),
            )

        async with self._jobs_lock:
            job = self._jobs[job_id]
            job.status = "running"
            job.message = "Training and validation started"
            job.started_at_us = now_us()
            slot.current_job_id = job_id
            slot.busy_intervals.append((time.monotonic(), None))

        self._touch_worker_heartbeat()
        await self._broadcast_workers()
        await self._broadcast_thread_slots()
        await self._broadcast_job_status(job)
        await self._broadcast_job_list()

        try:
            report = await asyncio.to_thread(
                run_pipeline,
                pipeline_args,
                emit_progress,
                lambda: self._job_stop_requested(job_id),
            )
        except PipelineCancelledError:
            shutil.rmtree(job_workspace, ignore_errors=True)
            async with self._jobs_lock:
                job = self._jobs[job_id]
                job.status = "cancelled"
                job.message = "Job cancelled during pipeline execution"
                job.finished_at_us = now_us()
                slot.cancelled_jobs += 1
            await self._finish_slot_run(slot)
            self._job_payloads.pop(job_id, None)
            self._job_requests.pop(job_id, None)
            self._job_stop_events.pop(job_id, None)
            await self._persist_state()
            self._touch_worker_heartbeat()
            await self._broadcast_workers()
            await self._broadcast_job_status(job)
            await self._broadcast_thread_slots()
            await self._broadcast_job_list()
            return
        except Exception as exc:  # pragma: no cover - defensive path
            shutil.rmtree(job_workspace, ignore_errors=True)
            async with self._jobs_lock:
                job = self._jobs[job_id]
                job.status = "failed"
                job.message = str(exc)
                job.error = str(exc)
                job.traceback_text = traceback.format_exc()
                job.finished_at_us = now_us()
                slot.failed_jobs += 1
            await self._finish_slot_run(slot)
            self._job_payloads.pop(job_id, None)
            self._job_requests.pop(job_id, None)
            self._job_stop_events.pop(job_id, None)
            await self._persist_state()
            self._touch_worker_heartbeat()
            await self._broadcast_workers()
            await self._broadcast_job_status(job)
            await self._broadcast_thread_slots()
            await self._broadcast_job_list()
            return

        durable_model_path = persist_selected_model_artifact(report, job_id)
        durable_bundle_path = persist_bundle_artifact(report, job_id)
        sanitized_report = sanitize_pipeline_report(
            report, durable_model_path, durable_bundle_path
        )
        shutil.rmtree(job_workspace, ignore_errors=True)
        async with self._jobs_lock:
            job = self._jobs[job_id]
            job.report = sanitized_report
            job.finished_at_us = now_us()
            if job.stop_requested:
                job.status = "completed"
                job.message = (
                    "Training completed after stop was requested; immediate "
                    "interruption is not yet supported for running jobs"
                )
            else:
                job.status = "completed"
                job.message = "Training and validation completed"
            slot.completed_jobs += 1

        await self._finish_slot_run(slot)
        self._job_payloads.pop(job_id, None)
        self._job_requests.pop(job_id, None)
        self._job_stop_events.pop(job_id, None)
        await self._persist_state()
        self._touch_worker_heartbeat()
        await self._broadcast_workers()
        await self._broadcast_job_status(job)
        await self._broadcast_thread_slots()
        await self._broadcast_job_list()

    def _job_stop_requested(self, job_id: str) -> bool:
        stop_event = self._job_stop_events.get(job_id)
        if stop_event is None:
            return True
        return stop_event.is_set()

    async def _finish_slot_run(self, slot: ThreadSlotState) -> None:
        if slot.busy_intervals:
            start, end = slot.busy_intervals[-1]
            if end is None:
                slot.busy_intervals[-1] = (start, time.monotonic())
        slot.current_job_id = None

    def _principal_can_manage_job(
        self,
        job: JobRecord,
        principal_id: str | None,
    ) -> bool:
        if job.owner_principal_id is None:
            return True
        if principal_id is None:
            return False
        return job.owner_principal_id == principal_id

    def _worker_can_inspect_job(
        self,
        job: JobRecord,
        worker_id: str | None,
    ) -> bool:
        normalized_worker_id = str(worker_id or "").strip()
        return bool(normalized_worker_id) and job.worker_id == normalized_worker_id

    def _active_jobs_for_slot(self, slot_id: str) -> list[JobRecord]:
        return [
            job
            for job in self._jobs.values()
            if job.thread_slot_id == slot_id
            and job.status not in TERMINAL_JOB_STATUSES
        ]

    def _resolved_worker_status(self, worker_id: str) -> str:
        worker = self._workers.get(worker_id)
        if worker is None:
            return "missing"
        return worker_status_from_heartbeat(
            worker_id=worker.worker_id,
            embedded_worker_id=self._worker_id,
            last_heartbeat_us=worker.last_heartbeat_us,
            status=worker.status,
        )

    def _runtime_worker_status(self, worker_id: str) -> str:
        worker = self._workers.get(worker_id)
        if worker is not None and worker.status == "draining":
            return "draining"
        return "online"

    async def _force_recover_remote_job(
        self,
        job: JobRecord,
        *,
        worker_status: str,
        automatic: bool = False,
    ) -> None:
        slot = self._slots.get(job.thread_slot_id)
        if slot is not None:
            if slot.current_job_id == job.job_id:
                await self._finish_slot_run(slot)
            slot.failed_jobs += 1
        job.status = "failed"
        if automatic:
            job.message = (
                f"Automatically recovered after worker {job.worker_id} remained "
                f"{worker_status}; the lost run must be resubmitted"
            )
        else:
            job.message = (
                f"Recovered after worker {job.worker_id} became {worker_status}; "
                "the lost run must be resubmitted"
            )
        job.error = f"worker {job.worker_id} became {worker_status}"
        job.finished_at_us = now_us()

        self._job_payloads.pop(job.job_id, None)
        self._job_requests.pop(job.job_id, None)
        stop_event = self._job_stop_events.pop(job.job_id, None)
        if stop_event is not None:
            stop_event.set()

    async def _auto_recover_stalled_workers(self) -> None:
        recovered_jobs: list[JobRecord] = []
        async with self._jobs_lock:
            sweep_now_us = now_us()
            for worker in sorted(self._workers.values(), key=lambda item: item.worker_id):
                if worker.worker_id == self._worker_id:
                    continue
                worker_status = self._resolved_worker_status(worker.worker_id)
                if not worker_status_allows_recovery(worker_status):
                    continue
                if not worker_is_past_auto_recover_deadline(
                    worker_id=worker.worker_id,
                    embedded_worker_id=self._worker_id,
                    last_heartbeat_us=worker.last_heartbeat_us,
                    now_wall_us=sweep_now_us,
                ):
                    continue
                active_jobs = sorted(
                    [
                        job
                        for job in self._jobs.values()
                        if job.worker_id == worker.worker_id
                        and job.status in RUNNING_JOB_STATUSES
                    ],
                    key=lambda item: (item.thread_slot_id, item.sequence, item.created_at_us),
                )
                for job in active_jobs:
                    await self._force_recover_remote_job(
                        job,
                        worker_status=worker_status,
                        automatic=True,
                    )
                    recovered_jobs.append(job)
        if not recovered_jobs:
            return
        for job in recovered_jobs:
            await self._broadcast_job_status(job)
        await self._persist_state()
        await self._broadcast_workers()
        await self._broadcast_thread_slots()
        await self._broadcast_job_list()

    async def _handle_get_job_status(
        self,
        websocket: ServerConnection,
        payload: dict[str, Any],
        request_id: Any,
    ) -> None:
        job_id = str(payload.get("job_id") or "")
        principal_id = self._resolve_scheduler_principal(websocket, payload)
        worker_id = str(payload.get("worker_id") or "").strip()
        async with self._jobs_lock:
            job = self._jobs.get(job_id)
        if job is None:
            if worker_id:
                await websocket.send(
                    json.dumps(
                        {
                            "type": "job_status",
                            "request_id": request_id,
                            "job_id": job_id,
                            "worker_id": worker_id,
                            "status": "failed",
                            "message": "Control plane no longer has state for this worker job",
                            "error": "job state missing after reconnect",
                        }
                    )
                )
                return
            await self._send_error(
                websocket,
                f"job_id not found: {job_id}",
                request_id=request_id,
            )
            return
        if not self._principal_can_manage_job(job, principal_id) and not self._worker_can_inspect_job(
            job, worker_id
        ):
            await self._send_error(
                websocket,
                (
                    f"principal {principal_id or '<unknown>'} and worker "
                    f"{worker_id or '<unknown>'} cannot inspect job {job_id}"
                ),
                request_id=request_id,
            )
            return
        await websocket.send(
            json.dumps(
                {
                    "type": "job_status",
                    "request_id": request_id,
                    **job_to_dict(job),
                }
            )
        )

    async def _handle_update_job(
        self,
        websocket: ServerConnection,
        payload: dict[str, Any],
        request_id: Any,
    ) -> None:
        job_id = str(payload.get("job_id") or "")
        priority_value = payload.get("priority")
        principal_id = self._resolve_scheduler_principal(websocket, payload)
        if priority_value is None:
            await self._send_error(
                websocket,
                "priority is required",
                request_id=request_id,
            )
            return

        async with self._jobs_lock:
            job = self._jobs.get(job_id)
            if job is None:
                await self._send_error(
                    websocket,
                    f"job_id not found: {job_id}",
                    request_id=request_id,
                )
                return
            if not self._principal_can_manage_job(job, principal_id):
                await self._send_error(
                    websocket,
                    f"principal {principal_id or '<unknown>'} cannot update job {job_id}",
                    request_id=request_id,
                )
                return
            if job.status != "queued":
                await self._send_error(
                    websocket,
                    "only queued jobs can be updated",
                    request_id=request_id,
                )
                return
            job.priority = int(priority_value)
            job.queue_token += 1
            slot = self._slots[job.thread_slot_id]
            await slot.queue.put(
                (-job.priority, job.sequence, job.job_id, job.queue_token)
            )
            job.message = f"Priority updated to {job.priority}"

        await self._broadcast_job_status(job)
        await self._persist_state()
        self._touch_worker_heartbeat()
        await self._broadcast_workers()
        await self._broadcast_thread_slots()
        await self._broadcast_job_list()
        await websocket.send(
            json.dumps(
                {
                    "type": "job_status",
                    "request_id": request_id,
                    **job_to_dict(job),
                }
            )
        )

    async def _handle_stop_job(
        self,
        websocket: ServerConnection,
        payload: dict[str, Any],
        request_id: Any,
    ) -> None:
        job_id = str(payload.get("job_id") or "")
        principal_id = self._resolve_scheduler_principal(websocket, payload)
        async with self._jobs_lock:
            job = self._jobs.get(job_id)
            if job is None:
                await self._send_error(
                    websocket,
                    f"job_id not found: {job_id}",
                    request_id=request_id,
                )
                return
            if not self._principal_can_manage_job(job, principal_id):
                await self._send_error(
                    websocket,
                    f"principal {principal_id or '<unknown>'} cannot stop job {job_id}",
                    request_id=request_id,
                )
                return

            if job.status == "queued":
                job.status = "cancelled"
                job.message = "Cancelled before execution"
                job.finished_at_us = now_us()
                job.queue_token += 1
                slot = self._slots[job.thread_slot_id]
                slot.cancelled_jobs += 1
                payload = self._job_payloads.pop(job_id, None)
                self._job_requests.pop(job_id, None)
                stop_event = self._job_stop_events.pop(job_id, None)
                if payload is not None:
                    _pipeline_args, job_workspace = payload
                    shutil.rmtree(job_workspace, ignore_errors=True)
                if stop_event is not None:
                    stop_event.set()
            elif job.status == "running":
                job.stop_requested = True
                job.status = "cancelling"
                job.message = (
                    "Stop requested; the runner will stop at the next pipeline "
                    "checkpoint"
                )
                stop_event = self._job_stop_events.get(job_id)
                if stop_event is not None:
                    stop_event.set()
            elif job.status == "cancelling":
                job.message = "Stop already requested"
            else:
                await self._send_error(
                    websocket,
                    f"job {job_id} is not stoppable in status {job.status}",
                    request_id=request_id,
                )
                return

        await self._broadcast_job_status(job)
        await self._persist_state()
        self._touch_worker_heartbeat()
        await self._broadcast_workers()
        await self._broadcast_thread_slots()
        await self._broadcast_job_list()
        await websocket.send(
            json.dumps(
                {
                    "type": "job_status",
                    "request_id": request_id,
                    **job_to_dict(job),
                }
            )
        )

    async def _handle_recover_job(
        self,
        websocket: ServerConnection,
        payload: dict[str, Any],
        request_id: Any,
    ) -> None:
        job_id = str(payload.get("job_id") or "")
        principal_id = self._resolve_scheduler_principal(websocket, payload)
        async with self._jobs_lock:
            job = self._jobs.get(job_id)
            if job is None:
                await self._send_error(
                    websocket,
                    f"job_id not found: {job_id}",
                    request_id=request_id,
                )
                return
            if not self._principal_can_manage_job(job, principal_id):
                await self._send_error(
                    websocket,
                    f"principal {principal_id or '<unknown>'} cannot recover job {job_id}",
                    request_id=request_id,
                )
                return
            if job.status not in RUNNING_JOB_STATUSES:
                await self._send_error(
                    websocket,
                    f"job {job_id} is not recoverable in status {job.status}",
                    request_id=request_id,
                )
                return
            if job.worker_id == self._worker_id:
                await self._send_error(
                    websocket,
                    "embedded worker jobs cannot be force-recovered",
                    request_id=request_id,
                )
                return

            worker_status = self._resolved_worker_status(job.worker_id)
            if not worker_status_allows_recovery(worker_status):
                await self._send_error(
                    websocket,
                    (
                        f"job {job_id} is attached to worker {job.worker_id} "
                        f"with status {worker_status} and cannot be recovered yet"
                    ),
                    request_id=request_id,
                )
                return

            await self._force_recover_remote_job(job, worker_status=worker_status)

        await self._broadcast_job_status(job)
        await self._persist_state()
        self._touch_worker_heartbeat()
        await self._broadcast_workers()
        await self._broadcast_thread_slots()
        await self._broadcast_job_list()
        await websocket.send(
            json.dumps(
                {
                    "type": "job_status",
                    "request_id": request_id,
                    **job_to_dict(job),
                }
            )
        )

    async def _handle_recover_worker(
        self,
        websocket: ServerConnection,
        payload: dict[str, Any],
        request_id: Any,
    ) -> None:
        worker_id = str(payload.get("worker_id") or "").strip()
        principal_id = self._resolve_scheduler_principal(websocket, payload)
        if not worker_id:
            await self._send_error(
                websocket,
                "worker_id is required",
                request_id=request_id,
            )
            return
        if worker_id == self._worker_id:
            await self._send_error(
                websocket,
                "embedded worker cannot be force-recovered",
                request_id=request_id,
            )
            return

        recovered_jobs: list[JobRecord] = []
        async with self._jobs_lock:
            worker_status = self._resolved_worker_status(worker_id)
            if not worker_status_allows_recovery(worker_status):
                await self._send_error(
                    websocket,
                    f"worker {worker_id} is in status {worker_status} and cannot be recovered yet",
                    request_id=request_id,
                )
                return

            active_jobs = sorted(
                [
                    job
                    for job in self._jobs.values()
                    if job.worker_id == worker_id and job.status in RUNNING_JOB_STATUSES
                ],
                key=lambda item: (item.thread_slot_id, item.sequence, item.created_at_us),
            )
            if not active_jobs:
                await self._send_error(
                    websocket,
                    f"worker {worker_id} has no recoverable active jobs",
                    request_id=request_id,
                )
                return
            for job in active_jobs:
                if not self._principal_can_manage_job(job, principal_id):
                    await self._send_error(
                        websocket,
                        (
                            f"principal {principal_id or '<unknown>'} cannot recover "
                            f"worker {worker_id} because job {job.job_id} is owned by "
                            f"{job.owner_principal_id or '<unassigned>'}"
                        ),
                        request_id=request_id,
                    )
                    return
            for job in active_jobs:
                await self._force_recover_remote_job(job, worker_status=worker_status)
                recovered_jobs.append(job)

        for job in recovered_jobs:
            await self._broadcast_job_status(job)
        await self._persist_state()
        self._touch_worker_heartbeat()
        await self._broadcast_workers()
        await self._broadcast_thread_slots()
        await self._broadcast_job_list()
        await self._send_workers(websocket, request_id=request_id, principal_id=None)

    async def _set_job_state(
        self,
        job_id: str,
        *,
        status: str,
        message: str,
    ) -> None:
        async with self._jobs_lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            if job.status in TERMINAL_JOB_STATUSES:
                return
            if job.stop_requested and status == "running":
                job.status = "cancelling"
                job.message = (
                    "Stop requested; current pipeline step is still finishing"
                )
            else:
                job.status = status
                job.message = message
        await self._broadcast_job_status(job)
        self._touch_worker_heartbeat()
        await self._broadcast_workers()
        await self._broadcast_thread_slots()

    def _ensure_local_job_payload(
        self,
        job: JobRecord,
    ) -> tuple[argparse.Namespace, Path] | None:
        payload = self._job_payloads.get(job.job_id)
        if payload is not None:
            return payload
        request_payload = self._job_requests.get(job.job_id)
        if not isinstance(request_payload, dict):
            return None
        job_workspace = create_job_workspace(self._base_args)
        try:
            pipeline_args = build_pipeline_namespace(
                self._base_args,
                request_payload,
                output_dir=str(job_workspace),
            )
        except Exception:
            shutil.rmtree(job_workspace, ignore_errors=True)
            raise
        self._job_stop_events.setdefault(job.job_id, threading.Event())
        payload = (pipeline_args, job_workspace)
        self._job_payloads[job.job_id] = payload
        return payload

    async def _run_slot(self, slot: ThreadSlotState) -> None:
        while True:
            _priority_key, _sequence, job_id, queue_token = await slot.queue.get()
            async with self._jobs_lock:
                job = self._jobs.get(job_id)
                if job is None:
                    continue
                if job.queue_token != queue_token:
                    continue
                if job.status != "queued":
                    continue
            if self._runtime_worker_status(slot.worker_id) == "draining":
                await slot.queue.put((_priority_key, _sequence, job_id, queue_token))
                await asyncio.sleep(SHARED_STATE_DB_POLL_INTERVAL_S)
                continue
            try:
                payload = self._ensure_local_job_payload(job)
            except Exception as exc:
                payload = None
                async with self._jobs_lock:
                    failed_job = self._jobs.get(job_id)
                    if failed_job is not None and failed_job.status == "queued":
                        failed_job.status = "failed"
                        failed_job.message = f"Failed to rebuild local job payload: {exc}"
                        failed_job.error = str(exc)
                        failed_job.traceback_text = traceback.format_exc()
                        failed_job.finished_at_us = now_us()
                        slot.failed_jobs += 1
                        self._job_requests.pop(job_id, None)
                        self._job_stop_events.pop(job_id, None)
                if failed_job is not None:
                    self._touch_worker_heartbeat()
                    await self._broadcast_workers()
                    await self._broadcast_job_status(failed_job)
                    await self._broadcast_thread_slots()
                    await self._broadcast_job_list()
                continue
            if payload is None:
                async with self._jobs_lock:
                    job = self._jobs.get(job_id)
                    if job is not None and job.status == "queued":
                        job.status = "failed"
                        job.message = "Scheduler payload missing"
                        job.error = "scheduler payload missing"
                        job.finished_at_us = now_us()
                        slot.failed_jobs += 1
                        self._job_payloads.pop(job_id, None)
                        self._job_requests.pop(job_id, None)
                        self._job_stop_events.pop(job_id, None)
                if job is not None:
                    self._touch_worker_heartbeat()
                    await self._broadcast_workers()
                    await self._broadcast_job_status(job)
                    await self._broadcast_thread_slots()
                    await self._broadcast_job_list()
                continue
            pipeline_args, job_workspace = payload
            await self._run_job_in_slot(slot, job_id, pipeline_args, job_workspace)

    def _touch_worker_heartbeat(
        self,
        worker_id: str | None = None,
        *,
        slot_count: int | None = None,
        source: str | None = None,
        status: str | None = None,
        session_id: str | None = None,
    ) -> None:
        target_worker_id = worker_id or self._worker_id
        heartbeat_us = now_us()
        if target_worker_id == self._worker_id:
            self._worker_last_heartbeat_us = heartbeat_us
        existing = self._workers.get(target_worker_id)
        resolved_slot_count = slot_count
        if resolved_slot_count is None:
            if existing is not None:
                resolved_slot_count = existing.slot_count
            elif target_worker_id == self._worker_id:
                resolved_slot_count = self._slot_count
            else:
                resolved_slot_count = 0
        if resolved_slot_count <= 0:
            return
        self._workers[target_worker_id] = WorkerNodeRecord(
            worker_id=target_worker_id,
            slot_count=resolved_slot_count,
            last_heartbeat_us=heartbeat_us,
            source=source or (existing.source if existing is not None else "external"),
            status=status or (existing.status if existing is not None else "online"),
            session_id=session_id or (existing.session_id if existing is not None else None),
        )

    def _worker_summaries(
        self,
        jobs: dict[str, JobRecord],
        *,
        principal_id: str | None,
        user: AuthenticatedSessionUser | None = None,
        show_all: bool = False,
    ) -> list[dict[str, Any]]:
        summary_now_wall_us = now_us()
        summary_now_monotonic = time.monotonic()
        return [
            worker_to_dict(
                worker.worker_id,
                self._slots.values(),
                jobs,
                last_heartbeat_us=worker.last_heartbeat_us,
                principal_id=principal_id,
                viewer_has_shared_compute_access=bool(user and user.shared_compute_access),
                viewer_is_admin=bool(user and user.is_admin),
                show_all=show_all,
                now_monotonic=summary_now_monotonic,
                now_wall_us=summary_now_wall_us,
                slot_count_override=worker.slot_count,
                worker_source=worker.source,
                worker_status=worker.status,
                embedded_worker_id=self._worker_id,
            )
            for worker in sorted(self._workers.values(), key=lambda item: item.worker_id)
        ]

    async def _worker_summary_payload(
        self,
        worker_id: str,
        *,
        principal_id: str | None,
        user: AuthenticatedSessionUser | None = None,
        show_all: bool = False,
    ) -> dict[str, Any] | None:
        worker = self._workers.get(worker_id)
        if worker is None:
            return None
        async with self._jobs_lock:
            jobs = dict(self._jobs)
        return worker_to_dict(
            worker.worker_id,
            self._slots.values(),
            jobs,
            last_heartbeat_us=worker.last_heartbeat_us,
            principal_id=principal_id,
            viewer_has_shared_compute_access=bool(user and user.shared_compute_access),
            viewer_is_admin=bool(user and user.is_admin),
            show_all=show_all,
            now_monotonic=time.monotonic(),
            now_wall_us=now_us(),
            slot_count_override=worker.slot_count,
            worker_source=worker.source,
            worker_status=worker.status,
            embedded_worker_id=self._worker_id,
        )

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL_S)
            await self._auto_recover_stalled_workers()
            self._touch_worker_heartbeat()
            await self._broadcast_workers()

    async def _send_workers(
        self,
        websocket: ServerConnection,
        *,
        request_id: Any,
        principal_id: Any = None,
        user: AuthenticatedSessionUser | None = None,
        show_all: bool = False,
    ) -> None:
        principal = str(principal_id).strip() if principal_id is not None else (
            user.username if user is not None and not (user.is_admin and show_all) else None
        )
        async with self._jobs_lock:
            jobs = dict(self._jobs)
        await websocket.send(
            json.dumps(
                {
                    "type": "workers",
                    "request_id": request_id,
                    "principal_id": principal,
                    "workers": self._worker_summaries(
                        jobs,
                        principal_id=principal,
                        user=user,
                        show_all=show_all,
                    ),
                }
            )
        )

    async def _send_thread_slots(
        self,
        websocket: ServerConnection,
        *,
        request_id: Any,
        principal_id: Any = None,
        user: AuthenticatedSessionUser | None = None,
        show_all: bool = False,
    ) -> None:
        principal = str(principal_id).strip() if principal_id is not None else (
            user.username if user is not None and not (user.is_admin and show_all) else None
        )
        async with self._jobs_lock:
            jobs = dict(self._jobs)
        now_monotonic = time.monotonic()
        slots = [
            slot_to_dict(slot, jobs, now_monotonic=now_monotonic)
            for slot in self._visible_slots(principal, user=user, show_all=show_all)
        ]
        await websocket.send(
            json.dumps(
                {
                    "type": "thread_slots",
                    "request_id": request_id,
                    "worker_id": self._worker_id,
                    "principal_id": principal,
                    "slots": slots,
                }
            )
        )

    async def _send_job_list(
        self,
        websocket: ServerConnection,
        *,
        request_id: Any,
        principal_id: Any,
        thread_slot_id: Any = None,
    ) -> None:
        principal = str(principal_id).strip() if principal_id is not None else None
        slot_id = str(thread_slot_id).strip() if thread_slot_id is not None else None
        async with self._jobs_lock:
            jobs = list(self._jobs.values())
        filtered_jobs = [
            job_to_dict(job)
            for job in sorted(
                jobs,
                key=lambda item: (
                    item.thread_slot_id,
                    item.status in TERMINAL_JOB_STATUSES,
                    -item.priority,
                    item.created_at_us,
                ),
            )
            if (principal is None or job.owner_principal_id == principal)
            and (slot_id is None or job.thread_slot_id == slot_id)
        ]
        await websocket.send(
            json.dumps(
                {
                    "type": "job_list",
                    "request_id": request_id,
                    "principal_id": principal,
                    "thread_slot_id": slot_id,
                    "jobs": filtered_jobs,
                }
            )
        )

    def _visible_slots(
        self,
        principal_id: str | None,
        *,
        user: AuthenticatedSessionUser | None = None,
        show_all: bool = False,
    ) -> list[ThreadSlotState]:
        slots = sorted(
            self._slots.values(),
            key=lambda item: (item.worker_id, item.slot_index),
        )
        if principal_id is None and user is None:
            return slots
        return [
            slot
            for slot in slots
            if self._slot_is_visible_to_user(slot, user, show_all=show_all)
            or (
                user is None
                and principal_id is not None
                and (
                    (slot.access_mode == "shared")
                    or slot.dedicated_username == principal_id
                )
            )
        ]

    async def _broadcast_job_status(self, job: JobRecord) -> None:
        if not self._clients:
            return
        payload = {"type": "job_status", **job_to_dict(job)}
        stale_clients: list[ServerConnection] = []
        for client in list(self._clients):
            user = self._connection_user(client)
            if user is not None and not user.is_admin:
                if job.owner_principal_id != user.username:
                    continue
            try:
                await client.send(json.dumps(payload))
            except Exception as exc:  # pragma: no cover - disconnect race
                LOG.warning("failed to broadcast job status: %s", exc)
                stale_clients.append(client)
        for client in stale_clients:
            self._client_users.pop(client, None)
            self._clients.discard(client)

    async def _broadcast_workers(self) -> None:
        async with self._jobs_lock:
            jobs = dict(self._jobs)
        stale_clients: list[ServerConnection] = []
        for client in list(self._clients):
            user = self._connection_user(client)
            principal_id = user.username if user is not None and not user.is_admin else None
            payload = {
                "type": "workers",
                "request_id": None,
                "principal_id": principal_id,
                "workers": self._worker_summaries(
                    jobs,
                    principal_id=principal_id,
                    user=user,
                    show_all=False,
                ),
            }
            try:
                await client.send(json.dumps(payload))
            except Exception as exc:  # pragma: no cover - disconnect race
                LOG.warning("failed to broadcast workers: %s", exc)
                stale_clients.append(client)
        for client in stale_clients:
            self._client_users.pop(client, None)
            self._clients.discard(client)

    async def _broadcast_thread_slots(self) -> None:
        async with self._jobs_lock:
            jobs = dict(self._jobs)
        now_monotonic = time.monotonic()
        stale_clients: list[ServerConnection] = []
        for client in list(self._clients):
            user = self._connection_user(client)
            principal_id = user.username if user is not None and not user.is_admin else None
            payload = {
                "type": "thread_slots",
                "request_id": None,
                "worker_id": self._worker_id,
                "principal_id": principal_id,
                "slots": [
                    slot_to_dict(slot, jobs, now_monotonic=now_monotonic)
                    for slot in self._visible_slots(principal_id, user=user, show_all=False)
                ],
            }
            try:
                await client.send(json.dumps(payload))
            except Exception as exc:  # pragma: no cover - disconnect race
                LOG.warning("failed to broadcast thread slots: %s", exc)
                stale_clients.append(client)
        for client in stale_clients:
            self._client_users.pop(client, None)
            self._clients.discard(client)

    async def _broadcast_job_list(self) -> None:
        async with self._jobs_lock:
            jobs = list(self._jobs.values())
        stale_clients: list[ServerConnection] = []
        sorted_jobs = sorted(
            jobs,
            key=lambda item: (
                item.thread_slot_id,
                item.status in TERMINAL_JOB_STATUSES,
                -item.priority,
                item.created_at_us,
            ),
        )
        for client in list(self._clients):
            user = self._connection_user(client)
            principal_id = user.username if user is not None and not user.is_admin else None
            payload = {
                "type": "job_list",
                "request_id": None,
                "principal_id": principal_id,
                "thread_slot_id": None,
                "jobs": [
                    job_to_dict(job)
                    for job in sorted_jobs
                    if principal_id is None or job.owner_principal_id == principal_id
                ],
            }
            try:
                await client.send(json.dumps(payload))
            except Exception as exc:  # pragma: no cover - disconnect race
                LOG.warning("failed to broadcast job list: %s", exc)
                stale_clients.append(client)
        for client in stale_clients:
            self._client_users.pop(client, None)
            self._clients.discard(client)

    async def _send_error(
        self,
        websocket: ServerConnection,
        message: str,
        *,
        request_id: Any = None,
    ) -> None:
        await websocket.send(
            json.dumps(
                {
                    "type": "error",
                    "request_id": request_id,
                    "message": message,
                }
            )
        )


async def _run_server(args: argparse.Namespace) -> None:
    import websockets

    server = MlControlPlaneServer(args)
    await server.start()
    async with websockets.serve(server.handle_client, args.host, args.port):
        LOG.info(
            "ML control plane listening on ws://%s:%s with worker_id=%s slots=%s",
            args.host,
            args.port,
            server._worker_id,
            server._slot_count,
        )
        await asyncio.Future()


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(_run_server(args))


if __name__ == "__main__":
    main()
