#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from pathlib import Path
import uuid
from typing import Any

from natkit_auth_shared import (
    apply_websocket_headers,
    build_websocket_cookie_header,
    derive_auth_base_url,
    resolve_session_token,
)
from natkit_ml_control_plane import DEFAULT_DRAIN_WAIT_TIMEOUT_S

LOG = logging.getLogger(__name__)


def add_result_json_out_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--result-json-out",
        help=(
            "Write the final structured command result to this JSON file. "
            "Parent directories are created automatically."
        ),
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Operate natKit ML workers through the control-plane WebSocket. "
            "This wrapper supports worker inspection plus drain/resume flows "
            "for deployment and maintenance automation."
        )
    )
    parser.add_argument(
        "--control-plane-url",
        default="ws://127.0.0.1:8786",
        help="WebSocket URL for the ML control plane",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level",
    )
    parser.add_argument(
        "--auth-base-url",
        default=os.getenv("NATKIT_AUTH_BASE_URL"),
        help="HTTP base URL for shared auth login; defaults to the backend on port 7409 for the control-plane host",
    )
    parser.add_argument(
        "--auth-session-token",
        default=os.getenv("NATKIT_AUTH_SESSION_TOKEN"),
        help="Existing shared auth session token to present to the control plane",
    )
    parser.add_argument(
        "--auth-username",
        default=os.getenv("NATKIT_AUTH_USERNAME"),
        help="Username to log in through the shared auth backend before connecting",
    )
    parser.add_argument(
        "--auth-password",
        default=os.getenv("NATKIT_AUTH_PASSWORD"),
        help="Password to log in through the shared auth backend before connecting",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print structured JSON output instead of human-readable text",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser(
        "list",
        help="List worker summaries from the ML control plane",
    )
    add_result_json_out_argument(list_parser)

    status_parser = subparsers.add_parser(
        "status",
        help="Show one worker summary from the ML control plane",
    )
    add_result_json_out_argument(status_parser)
    status_parser.add_argument(
        "--worker-id",
        dest="worker_ids",
        action="append",
        help="Worker id to inspect; repeat for multiple workers",
    )
    status_parser.add_argument(
        "--workers-file",
        help="Path to a newline-delimited worker-id file; blank lines and # comments are ignored",
    )
    status_parser.add_argument(
        "--workers-json",
        help=(
            "Path to a JSON worker manifest. Supported shapes: "
            '["worker-a"], {"workers": ["worker-a", {"worker_id": "worker-b"}]}'
        ),
    )
    status_parser.add_argument(
        "--worker-group",
        dest="worker_groups",
        action="append",
        help="Filter --workers-json entries by group; repeat for multiple groups",
    )

    drain_parser = subparsers.add_parser(
        "drain",
        help="Mark a worker draining and optionally wait until it is ready to stop",
    )
    add_result_json_out_argument(drain_parser)
    drain_parser.add_argument(
        "--worker-id",
        dest="worker_ids",
        action="append",
        help="Worker id to drain; repeat for multiple workers",
    )
    drain_parser.add_argument(
        "--workers-file",
        help="Path to a newline-delimited worker-id file; blank lines and # comments are ignored",
    )
    drain_parser.add_argument(
        "--workers-json",
        help=(
            "Path to a JSON worker manifest. Supported shapes: "
            '["worker-a"], {"workers": ["worker-a", {"worker_id": "worker-b"}]}'
        ),
    )
    drain_parser.add_argument(
        "--worker-group",
        dest="worker_groups",
        action="append",
        help="Filter --workers-json entries by group; repeat for multiple groups",
    )
    drain_parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Return after setting draining status instead of waiting for readiness",
    )
    drain_parser.add_argument(
        "--timeout-s",
        type=float,
        default=DEFAULT_DRAIN_WAIT_TIMEOUT_S,
        help="Maximum seconds to wait for drain readiness",
    )
    drain_parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop after the first request error instead of continuing through the worker list",
    )

    resume_parser = subparsers.add_parser(
        "resume",
        help="Mark a worker online again so it can accept new work",
    )
    add_result_json_out_argument(resume_parser)
    resume_parser.add_argument(
        "--worker-id",
        dest="worker_ids",
        action="append",
        help="Worker id to resume; repeat for multiple workers",
    )
    resume_parser.add_argument(
        "--workers-file",
        help="Path to a newline-delimited worker-id file; blank lines and # comments are ignored",
    )
    resume_parser.add_argument(
        "--workers-json",
        help=(
            "Path to a JSON worker manifest. Supported shapes: "
            '["worker-a"], {"workers": ["worker-a", {"worker_id": "worker-b"}]}'
        ),
    )
    resume_parser.add_argument(
        "--worker-group",
        dest="worker_groups",
        action="append",
        help="Filter --workers-json entries by group; repeat for multiple groups",
    )
    resume_parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop after the first request error instead of continuing through the worker list",
    )

    return parser.parse_args(argv)


def build_request_id() -> str:
    return str(uuid.uuid4())


def load_worker_ids_from_file(path: str | Path) -> list[str]:
    worker_ids: list[str] = []
    file_path = Path(path)
    for raw_line in file_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        worker_ids.append(line)
    return worker_ids


def _matches_worker_group(
    entry: dict[str, Any],
    *,
    worker_groups: set[str],
) -> bool:
    if not worker_groups:
        return True
    raw_groups = entry.get("groups", entry.get("group"))
    if isinstance(raw_groups, str):
        return raw_groups in worker_groups
    if isinstance(raw_groups, list):
        return any(str(item).strip() in worker_groups for item in raw_groups)
    return False


def load_worker_ids_from_json_file(
    path: str | Path,
    *,
    worker_groups: list[str] | None = None,
) -> list[str]:
    file_path = Path(path)
    payload = json.loads(file_path.read_text(encoding="utf-8"))
    raw_workers: Any = payload
    if isinstance(payload, dict):
        raw_workers = payload.get("workers")
    if not isinstance(raw_workers, list):
        raise RuntimeError(
            "workers JSON must be a list or an object containing a workers[] list"
        )

    normalized_groups = {
        str(group).strip()
        for group in (worker_groups or [])
        if str(group).strip()
    }
    worker_ids: list[str] = []
    for item in raw_workers:
        if isinstance(item, str):
            if normalized_groups:
                continue
            normalized = item.strip()
            if normalized:
                worker_ids.append(normalized)
            continue
        if not isinstance(item, dict):
            raise RuntimeError(
                "workers JSON entries must be strings or objects with worker_id"
            )
        if not _matches_worker_group(item, worker_groups=normalized_groups):
            continue
        worker_id = str(item.get("worker_id") or "").strip()
        if not worker_id:
            raise RuntimeError("workers JSON object entries must include worker_id")
        worker_ids.append(worker_id)
    return worker_ids


def resolve_worker_ids(args: argparse.Namespace) -> list[str]:
    worker_ids = list(getattr(args, "worker_ids", []) or [])
    workers_file = getattr(args, "workers_file", None)
    if workers_file:
        worker_ids.extend(load_worker_ids_from_file(workers_file))
    workers_json = getattr(args, "workers_json", None)
    if workers_json:
        worker_ids.extend(
            load_worker_ids_from_json_file(
                workers_json,
                worker_groups=list(getattr(args, "worker_groups", []) or []),
            )
        )
    seen: set[str] = set()
    resolved: list[str] = []
    for worker_id in worker_ids:
        normalized = str(worker_id).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        resolved.append(normalized)
    return resolved


def worker_summary_from_workers_message(
    payload: dict[str, Any],
    *,
    worker_id: str,
) -> dict[str, Any]:
    workers = payload.get("workers")
    if not isinstance(workers, list):
        raise RuntimeError("workers response payload is missing workers[]")
    for worker in workers:
        if isinstance(worker, dict) and str(worker.get("worker_id") or "") == worker_id:
            return worker
    raise RuntimeError(f"worker_id not found in workers response: {worker_id}")


def workers_from_workers_message(payload: dict[str, Any]) -> list[dict[str, Any]]:
    workers = payload.get("workers")
    if not isinstance(workers, list):
        raise RuntimeError("workers response payload is missing workers[]")
    return [worker for worker in workers if isinstance(worker, dict)]


def worker_summaries_from_workers_message(
    payload: dict[str, Any],
    *,
    worker_ids: list[str],
) -> list[dict[str, Any]]:
    workers_by_id = {
        str(worker.get("worker_id") or ""): worker
        for worker in workers_from_workers_message(payload)
    }
    selected: list[dict[str, Any]] = []
    for worker_id in worker_ids:
        worker = workers_by_id.get(worker_id)
        if worker is None:
            raise RuntimeError(f"worker_id not found in workers response: {worker_id}")
        selected.append(worker)
    return selected


def result_worker_ids(results: list[dict[str, Any]]) -> list[str]:
    worker_ids: list[str] = []
    for item in results:
        worker_id = str(item.get("worker_id") or "").strip()
        if worker_id:
            worker_ids.append(worker_id)
    return worker_ids


def write_result_json(path: str | Path, payload: dict[str, Any]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_name(
        f".{output_path.name}.{uuid.uuid4().hex}.tmp"
    )
    temp_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temp_path.replace(output_path)


async def request_response(
    websocket: Any,
    payload: dict[str, Any],
    *,
    expected_types: set[str] | None = None,
) -> dict[str, Any]:
    request_id = build_request_id()
    outbound = dict(payload)
    outbound["request_id"] = request_id
    await websocket.send(json.dumps(outbound))
    while True:
        raw = await websocket.recv()
        message = json.loads(raw)
        if str(message.get("request_id") or "") != request_id:
            continue
        message_type = str(message.get("type") or "")
        if message_type == "error":
            raise RuntimeError(str(message.get("message") or "control-plane error"))
        if expected_types is not None and message_type not in expected_types:
            raise RuntimeError(
                f"unexpected response type {message_type!r}; expected one of {sorted(expected_types)!r}"
            )
        return message


async def run_operator_command(
    args: argparse.Namespace,
    *,
    connect_factory: Any | None = None,
) -> tuple[int, dict[str, Any]]:
    if args.command == "drain" and float(args.timeout_s) < 0:
        raise RuntimeError("timeout_s must be greater than or equal to zero")

    if connect_factory is None:
        import websockets

        connect_factory = websockets.connect

    auth_base_url = args.auth_base_url or derive_auth_base_url(args.control_plane_url)
    session_token = resolve_session_token(
        session_token=args.auth_session_token,
        username=args.auth_username,
        password=args.auth_password,
        auth_base_url=auth_base_url,
    )
    if not session_token:
        raise RuntimeError(
            "shared auth is required; provide --auth-session-token or --auth-username/--auth-password"
        )
    websocket_headers = build_websocket_cookie_header(session_token=session_token)
    connect_kwargs = apply_websocket_headers(connect_factory, websocket_headers)

    worker_ids = resolve_worker_ids(args)

    async with connect_factory(args.control_plane_url, **connect_kwargs) as websocket:
        if args.command == "list":
            workers_message = await request_response(
                websocket,
                {"action": "list_workers"},
                expected_types={"workers"},
            )
            return 0, {
                "command": "list",
                "workers": workers_from_workers_message(workers_message),
            }

        if not worker_ids:
            raise RuntimeError("at least one worker id must be provided")

        if args.command == "status":
            workers_message = await request_response(
                websocket,
                {"action": "list_workers"},
                expected_types={"workers"},
            )
            workers = worker_summaries_from_workers_message(
                workers_message,
                worker_ids=worker_ids,
            )
            if len(workers) == 1:
                return 0, {
                    "command": "status",
                    "requested_worker_ids": worker_ids,
                    "processed_worker_ids": worker_ids,
                    "worker": workers[0],
                }
            return 0, {
                "command": "status",
                "requested_worker_ids": worker_ids,
                "processed_worker_ids": worker_ids,
                "workers": workers,
            }

        if args.command == "resume":
            results: list[dict[str, Any]] = []
            had_error = False
            for worker_id in worker_ids:
                try:
                    workers_message = await request_response(
                        websocket,
                        {
                            "action": "update_worker_status",
                            "worker_id": worker_id,
                            "status": "online",
                        },
                        expected_types={"workers"},
                    )
                    worker = worker_summary_from_workers_message(
                        workers_message,
                        worker_id=worker_id,
                    )
                    results.append(worker)
                except Exception as exc:
                    had_error = True
                    results.append({"worker_id": worker_id, "error": str(exc)})
                    if bool(getattr(args, "fail_fast", False)):
                        break
            exit_code = 1 if had_error else 0
            if len(results) == 1:
                return exit_code, {
                    "command": "resume",
                    "requested_worker_ids": worker_ids,
                    "processed_worker_ids": result_worker_ids(results),
                    "worker": results[0],
                }
            return exit_code, {
                "command": "resume",
                "requested_worker_ids": worker_ids,
                "processed_worker_ids": result_worker_ids(results),
                "workers": results,
            }

        results = []
        had_error = False
        had_timeout = False
        for worker_id in worker_ids:
            try:
                workers_message = await request_response(
                    websocket,
                    {
                        "action": "update_worker_status",
                        "worker_id": worker_id,
                        "status": "draining",
                    },
                    expected_types={"workers"},
                )
                worker = worker_summary_from_workers_message(
                    workers_message,
                    worker_id=worker_id,
                )
                if args.no_wait:
                    results.append(worker)
                    continue
                drain_status = await request_response(
                    websocket,
                    {
                        "action": "wait_worker_drain_ready",
                        "worker_id": worker_id,
                        "timeout_s": float(args.timeout_s),
                    },
                    expected_types={"worker_drain_status"},
                )
                had_timeout = had_timeout or bool(drain_status.get("timed_out"))
                results.append(drain_status)
            except Exception as exc:
                had_error = True
                results.append({"worker_id": worker_id, "error": str(exc)})
                if bool(getattr(args, "fail_fast", False)):
                    break
        exit_code = 1 if had_error else (2 if had_timeout else 0)
        if len(results) == 1:
            return exit_code, {
                "command": "drain",
                "requested_worker_ids": worker_ids,
                "processed_worker_ids": result_worker_ids(results),
                "waited": not bool(args.no_wait),
                "worker": results[0],
            }
        return exit_code, {
            "command": "drain",
            "requested_worker_ids": worker_ids,
            "processed_worker_ids": result_worker_ids(results),
            "waited": not bool(args.no_wait),
            "workers": results,
        }


def format_human_output(result: dict[str, Any]) -> str:
    command = str(result.get("command") or "")
    if "workers" in result:
        workers = result.get("workers") or []
        if not workers:
            return "no workers"
        lines: list[str] = []
        for worker in workers:
            worker_id = str(worker.get("worker_id") or "<unknown>")
            error = str(worker.get("error") or "")
            if error:
                lines.append(f"{worker_id} error={error}")
                continue
            worker_status = str(worker.get("worker_status") or "unknown")
            active_jobs = int(worker.get("assigned_job_count") or 0)
            if command == "resume":
                lines.append(f"{worker_id} resumed with status {worker_status}")
                continue
            if command == "drain":
                if not bool(result.get("waited")):
                    lines.append(f"{worker_id} set to {worker_status}")
                    continue
                drain_ready = bool(worker.get("drain_ready"))
                remaining = int(worker.get("drain_remaining_job_count") or 0)
                timed_out = bool(worker.get("timed_out"))
                if drain_ready:
                    lines.append(f"{worker_id} is draining and ready to stop")
                elif timed_out:
                    lines.append(
                        f"{worker_id} is still draining after timeout with {remaining} active job(s) remaining"
                    )
                else:
                    lines.append(
                        f"{worker_id} is draining with {remaining} active job(s) remaining"
                    )
                continue
            drain_ready = bool(worker.get("drain_ready"))
            drain_suffix = ""
            if worker_status == "draining":
                drain_suffix = " ready" if drain_ready else (
                    f" {int(worker.get('drain_remaining_job_count') or 0)} remaining"
                )
            lines.append(f"{worker_id} {worker_status}{drain_suffix} active={active_jobs}")
        return "\n".join(lines)

    worker = result.get("worker") or {}
    worker_id = str(worker.get("worker_id") or "<unknown>")
    error = str(worker.get("error") or "")
    if error:
        return f"{worker_id} error={error}"
    worker_status = str(worker.get("worker_status") or "unknown")
    if command == "status":
        active_jobs = int(worker.get("assigned_job_count") or 0)
        if worker_status == "draining":
            if bool(worker.get("drain_ready")):
                return f"{worker_id} status={worker_status} active={active_jobs} drain=ready"
            remaining = int(worker.get("drain_remaining_job_count") or 0)
            return (
                f"{worker_id} status={worker_status} active={active_jobs} "
                f"drain={remaining} remaining"
            )
        return f"{worker_id} status={worker_status} active={active_jobs}"
    if command == "resume":
        return f"{worker_id} resumed with status {worker_status}"
    if not bool(result.get("waited")):
        return f"{worker_id} set to {worker_status}"
    drain_ready = bool(worker.get("drain_ready"))
    remaining = int(worker.get("drain_remaining_job_count") or 0)
    timed_out = bool(worker.get("timed_out"))
    if drain_ready:
        return f"{worker_id} is draining and ready to stop"
    if timed_out:
        return (
            f"{worker_id} is still draining after timeout with "
            f"{remaining} active job(s) remaining"
        )
    return f"{worker_id} is draining with {remaining} active job(s) remaining"


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        exit_code, result = asyncio.run(run_operator_command(args))
        output_payload = {"ok": exit_code == 0, "exit_code": exit_code, **result}
    except Exception as exc:
        output_payload = {"ok": False, "exit_code": 1, "error": str(exc)}
        result_json_out = getattr(args, "result_json_out", None)
        if result_json_out:
            write_result_json(result_json_out, output_payload)
        if args.json:
            print(json.dumps(output_payload))
        else:
            print(f"error: {exc}")
        return 1

    result_json_out = getattr(args, "result_json_out", None)
    if result_json_out:
        write_result_json(result_json_out, output_payload)
    if args.json:
        print(json.dumps(output_payload))
    else:
        print(format_human_output(result))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
