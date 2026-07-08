#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from pathlib import Path
import signal
import shutil
import sys
import threading
import traceback
import uuid
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
NATVR_SRC = REPO_ROOT / "natVR" / "src"
if str(NATVR_SRC) not in sys.path:
    sys.path.insert(0, str(NATVR_SRC))

from natvr.kafka_train_validate import PipelineCancelledError, run_pipeline

from natkit_auth_shared import (
    apply_websocket_headers,
    build_websocket_cookie_header,
    derive_auth_base_url,
    resolve_session_token,
)
from natkit_ml_control_plane import (
    build_pipeline_namespace,
    build_thread_slot_id,
    create_job_workspace,
    default_worker_id,
    parse_args as parse_control_plane_args,
    resolve_worker_thread_count,
    sanitize_pipeline_report,
)

LOG = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a standalone natKit ML worker that registers with the "
            "ML control plane, claims queued jobs for its slots, and "
            "executes the current natVR train/validate pipeline."
        )
    )
    parser.add_argument("--control-plane-url", default="ws://127.0.0.1:8786")
    parser.add_argument("--auth-base-url", default=os.getenv("NATKIT_AUTH_BASE_URL"))
    parser.add_argument("--auth-session-token", default=os.getenv("NATKIT_AUTH_SESSION_TOKEN"))
    parser.add_argument("--auth-username", default=os.getenv("NATKIT_AUTH_USERNAME"))
    parser.add_argument("--auth-password", default=os.getenv("NATKIT_AUTH_PASSWORD"))
    parser.add_argument("--worker-id")
    parser.add_argument("--worker-threads", type=int)
    parser.add_argument("--scratch-root")
    parser.add_argument("--claim-poll-interval-s", type=float, default=0.5)
    parser.add_argument("--heartbeat-interval-s", type=float, default=5.0)
    parser.add_argument("--reconnect-delay-s", type=float, default=1.0)
    parser.add_argument("--max-reconnect-delay-s", type=float, default=10.0)
    parser.add_argument("--shutdown-grace-period-s", type=float, default=5.0)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def build_worker_pipeline_base_args(args: argparse.Namespace) -> argparse.Namespace:
    base_args = parse_control_plane_args([])
    base_args.worker_id = args.worker_id
    base_args.worker_threads = args.worker_threads
    base_args.scratch_root = args.scratch_root
    return base_args


def build_worker_slot_ids(worker_id: str, slot_count: int) -> list[str]:
    return [build_thread_slot_id(worker_id, slot_index) for slot_index in range(slot_count)]


def worker_job_should_stop(job_payload: dict[str, Any]) -> bool:
    if bool(job_payload.get("stop_requested")):
        return True
    return str(job_payload.get("status") or "") in {"cancelling", "cancelled"}


def normalize_reconnect_delay(delay_s: float) -> float:
    return max(0.1, float(delay_s))


def normalize_max_reconnect_delay(initial_delay_s: float, max_delay_s: float) -> float:
    return max(normalize_reconnect_delay(initial_delay_s), float(max_delay_s))


class MlWorkerClient:
    def __init__(self, args: argparse.Namespace) -> None:
        self._args = args
        self._worker_id = args.worker_id or default_worker_id()
        self._worker_session_id = uuid.uuid4().hex
        self._slot_count = resolve_worker_thread_count(args.worker_threads)
        self._slot_ids = build_worker_slot_ids(self._worker_id, self._slot_count)
        self._pipeline_base_args = build_worker_pipeline_base_args(args)
        self._auth_base_url = args.auth_base_url or derive_auth_base_url(args.control_plane_url)
        self._session_token = resolve_session_token(
            session_token=args.auth_session_token,
            username=args.auth_username,
            password=args.auth_password,
            auth_base_url=self._auth_base_url,
        )
        if not self._session_token:
            raise RuntimeError(
                "shared auth is required; provide --auth-session-token or --auth-username/--auth-password"
            )
        self._pending_requests: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._request_send_lock = asyncio.Lock()
        self._active_slot_tasks: dict[str, asyncio.Task[None]] = {}
        self._active_job_stop_events: dict[str, threading.Event] = {}
        self._ws: Any = None
        self._receiver_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._shutdown_event = asyncio.Event()
        self._stop_requested = asyncio.Event()
        self._abandoned_inflight_work = False
        self._deferred_terminal_reports: dict[str, dict[str, Any]] = {}

    async def run(self) -> None:
        import websockets

        reconnect_delay_s = normalize_reconnect_delay(self._args.reconnect_delay_s)
        max_reconnect_delay_s = normalize_max_reconnect_delay(
            reconnect_delay_s,
            self._args.max_reconnect_delay_s,
        )
        while not self._stop_requested.is_set():
            self._reset_connection_state()
            try:
                connect_kwargs = apply_websocket_headers(
                    websockets.connect,
                    build_websocket_cookie_header(session_token=self._session_token),
                )
                async with websockets.connect(
                    self._args.control_plane_url,
                    **connect_kwargs,
                ) as websocket:
                    LOG.info(
                        "connected worker %s to control plane %s",
                        self._worker_id,
                        self._args.control_plane_url,
                    )
                    await self._run_connected_session(websocket)
                    reconnect_delay_s = normalize_reconnect_delay(
                        self._args.reconnect_delay_s
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - connection retry path
                LOG.warning("worker session ended: %s", exc)
            if self._stop_requested.is_set():
                return
            LOG.info(
                "reconnecting worker %s in %.1fs",
                self._worker_id,
                reconnect_delay_s,
            )
            try:
                await asyncio.wait_for(
                    self._stop_requested.wait(),
                    timeout=reconnect_delay_s,
                )
                return
            except asyncio.TimeoutError:
                pass
            reconnect_delay_s = min(max_reconnect_delay_s, reconnect_delay_s * 2.0)

    def _reset_connection_state(self) -> None:
        self._ws = None
        self._receiver_task = None
        self._heartbeat_task = None
        self._shutdown_event = asyncio.Event()
        self._pending_requests.clear()

    async def _run_connected_session(self, websocket: Any) -> None:
        self._ws = websocket
        self._receiver_task = asyncio.create_task(self._receiver_loop())
        await self._register_worker()
        await self._sync_slots()
        await self._sync_active_job_statuses()
        await self._flush_deferred_terminal_reports()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        try:
            await self._claim_loop()
        finally:
            if self._stop_requested.is_set():
                await self._drain_active_jobs(
                    timeout_s=self._args.shutdown_grace_period_s
                )
            await self._close_connected_session()

    def request_shutdown(self) -> None:
        if self._stop_requested.is_set():
            return
        LOG.info("shutdown requested for worker %s", self._worker_id)
        self._stop_requested.set()
        self._shutdown_event.set()
        for stop_event in self._active_job_stop_events.values():
            stop_event.set()

    async def _close_connected_session(self) -> None:
        self._shutdown_event.set()
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            await asyncio.gather(self._heartbeat_task, return_exceptions=True)
        reconnectable_inflight_work = self._has_reconnectable_inflight_work()
        try:
            if (
                self._receiver_task is not None
                and not self._receiver_task.done()
                and not reconnectable_inflight_work
            ):
                await self._request(
                    {
                        "action": "unregister_worker",
                        "worker_id": self._worker_id,
                        "worker_session_id": self._worker_session_id,
                    }
                )
            elif reconnectable_inflight_work:
                LOG.info(
                    "skipping unregister for worker %s while %d job task(s) or %d deferred terminal report(s) remain in flight",
                    self._worker_id,
                    len(self._active_slot_tasks),
                    len(self._deferred_terminal_reports),
                )
        except Exception as exc:  # pragma: no cover - disconnect cleanup path
            LOG.warning("failed to unregister worker %s: %s", self._worker_id, exc)
        if self._receiver_task is not None:
            self._receiver_task.cancel()
            await asyncio.gather(self._receiver_task, return_exceptions=True)

    async def _drain_active_jobs(self, *, timeout_s: float | None = None) -> bool:
        tasks = list(self._active_slot_tasks.values())
        if not tasks:
            return True
        await self._set_worker_status("draining")
        for stop_event in self._active_job_stop_events.values():
            stop_event.set()
        drain_timeout_s = (
            max(0.0, float(timeout_s))
            if timeout_s is not None
            else max(0.0, float(self._args.shutdown_grace_period_s))
        )
        done, pending = await asyncio.wait(tasks, timeout=drain_timeout_s)
        if done:
            await asyncio.gather(*done, return_exceptions=True)
        self._active_slot_tasks = {
            slot_id: task
            for slot_id, task in self._active_slot_tasks.items()
            if not task.done()
        }
        if pending:
            self._abandoned_inflight_work = True
            LOG.warning(
                "cancelling %d claimed job task(s) after %.1fs shutdown grace period",
                len(pending),
                drain_timeout_s,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            self._active_slot_tasks = {
                slot_id: task
                for slot_id, task in self._active_slot_tasks.items()
                if not task.done()
            }
            await self._set_worker_status("offline")
            return False
        return True

    def _has_reconnectable_inflight_work(self) -> bool:
        self._active_slot_tasks = {
            slot_id: task
            for slot_id, task in self._active_slot_tasks.items()
            if not task.done()
        }
        return bool(
            self._active_slot_tasks
            or self._deferred_terminal_reports
            or self._abandoned_inflight_work
        )

    async def _set_worker_status(self, status: str) -> None:
        if self._connection_is_unavailable():
            return
        try:
            await self._request(
                {
                    "action": "worker_heartbeat",
                    "worker_id": self._worker_id,
                    "slot_count": self._slot_count,
                    "status": status,
                    "worker_session_id": self._worker_session_id,
                }
            )
        except Exception as exc:
            LOG.warning(
                "failed to set worker %s status to %s: %s",
                self._worker_id,
                status,
                exc,
            )

    async def _receiver_loop(self) -> None:
        assert self._ws is not None
        try:
            async for raw in self._ws:
                message = json.loads(raw)
                request_id = message.get("request_id")
                if request_id is not None and request_id in self._pending_requests:
                    future = self._pending_requests.pop(str(request_id))
                    if not future.done():
                        future.set_result(message)
                    continue
                self._handle_unsolicited_message(message)
        finally:
            self._shutdown_event.set()
            for future in self._pending_requests.values():
                if not future.done():
                    future.set_exception(RuntimeError("control-plane connection closed"))
            self._pending_requests.clear()

    def _handle_unsolicited_message(self, message: dict[str, Any]) -> None:
        if message.get("type") != "job_status":
            return
        job_id = str(message.get("job_id") or "")
        stop_event = self._active_job_stop_events.get(job_id)
        if stop_event is None:
            return
        if worker_job_should_stop(message):
            stop_event.set()

    def _connection_is_unavailable(self) -> bool:
        if self._ws is None:
            return True
        if self._shutdown_event.is_set():
            return True
        if self._receiver_task is not None and self._receiver_task.done():
            return True
        return False

    async def _request(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._ws is None:
            raise RuntimeError("worker is not connected")
        request_id = str(uuid.uuid4())
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending_requests[request_id] = future
        outbound = dict(payload)
        outbound["request_id"] = request_id
        async with self._request_send_lock:
            await self._ws.send(json.dumps(outbound))
        response = await future
        if response.get("type") == "error":
            raise RuntimeError(str(response.get("message") or "control-plane error"))
        return response

    async def _flush_deferred_terminal_reports(self) -> None:
        if not self._deferred_terminal_reports:
            return
        pending_job_ids = sorted(self._deferred_terminal_reports.keys())
        for job_id in pending_job_ids:
            payload = self._deferred_terminal_reports.get(job_id)
            if payload is None:
                continue
            try:
                await self._request(payload)
            except Exception as exc:
                LOG.warning(
                    "failed to flush deferred terminal report for %s: %s",
                    job_id,
                    exc,
                )
                return
            self._deferred_terminal_reports.pop(job_id, None)

    def _state_update_conflict_requires_stop(self, message: str) -> bool:
        normalized_message = str(message or "").strip()
        return normalized_message.startswith("job_id not found:") or "is already terminal in status" in normalized_message

    async def _sync_active_job_statuses(self) -> None:
        if not self._active_job_stop_events:
            return
        for job_id, stop_event in list(self._active_job_stop_events.items()):
            try:
                response = await self._request(
                    {
                        "action": "get_job_status",
                        "worker_id": self._worker_id,
                        "job_id": job_id,
                    }
                )
            except Exception as exc:
                if self._state_update_conflict_requires_stop(str(exc)):
                    LOG.warning(
                        "stopping active job %s after reconnect because control-plane state is %s",
                        job_id,
                        exc,
                    )
                    stop_event.set()
                    continue
                raise
            if worker_job_should_stop(response) or str(response.get("status") or "") in {
                "completed",
                "failed",
                "cancelled",
            }:
                stop_event.set()
                continue
            await self._report_job_state(
                job_id,
                status="running",
                message="External worker reconnected and confirmed active execution",
            )

    async def _register_worker(self) -> None:
        await self._request(
            {
                "action": "register_worker",
                "worker_id": self._worker_id,
                "slot_count": self._slot_count,
                "status": "online",
                "worker_session_id": self._worker_session_id,
            }
        )

    async def _sync_slots(self) -> None:
        await self._request(
            {
                "action": "sync_worker_slots",
                "worker_id": self._worker_id,
                "slot_count": self._slot_count,
                "worker_session_id": self._worker_session_id,
            }
        )

    async def _heartbeat_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self._args.heartbeat_interval_s,
                )
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self._request(
                    {
                        "action": "worker_heartbeat",
                        "worker_id": self._worker_id,
                        "slot_count": self._slot_count,
                        "status": "online",
                        "worker_session_id": self._worker_session_id,
                    }
                )
            except Exception as exc:
                if self._shutdown_event.is_set():
                    return
                LOG.warning("worker heartbeat failed: %s", exc)
                self._shutdown_event.set()
                return

    async def _claim_loop(self) -> None:
        while not self._shutdown_event.is_set():
            for slot_id in self._slot_ids:
                if self._shutdown_event.is_set():
                    return
                current_task = self._active_slot_tasks.get(slot_id)
                if current_task is not None:
                    if current_task.done():
                        await asyncio.gather(current_task, return_exceptions=True)
                        self._active_slot_tasks.pop(slot_id, None)
                    else:
                        continue
                try:
                    response = await self._request(
                        {
                            "action": "claim_worker_slot_job",
                            "worker_id": self._worker_id,
                            "slot_id": slot_id,
                            "worker_session_id": self._worker_session_id,
                        }
                    )
                except Exception as exc:
                    if self._shutdown_event.is_set():
                        return
                    LOG.warning("failed to claim slot job for %s: %s", slot_id, exc)
                    self._shutdown_event.set()
                    return
                job_payload = response.get("job")
                if job_payload is None:
                    continue
                task = asyncio.create_task(self._run_claimed_job(slot_id, job_payload))
                self._active_slot_tasks[slot_id] = task
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self._args.claim_poll_interval_s,
                )
                return
            except asyncio.TimeoutError:
                pass

    async def _report_job_state(
        self,
        job_id: str,
        *,
        status: str,
        message: str,
        report: dict[str, Any] | None = None,
        error: str | None = None,
        traceback_text: str | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "action": "report_worker_job_state",
            "worker_id": self._worker_id,
            "worker_session_id": self._worker_session_id,
            "job_id": job_id,
            "status": status,
            "message": message,
        }
        if report is not None:
            payload["report"] = report
        if error is not None:
            payload["error"] = error
        if traceback_text is not None:
            payload["traceback_text"] = traceback_text
        try:
            await self._request(payload)
        except Exception as exc:
            if status == "running":
                if self._state_update_conflict_requires_stop(str(exc)):
                    stop_event = self._active_job_stop_events.get(job_id)
                    if stop_event is not None:
                        stop_event.set()
                    LOG.warning(
                        "control-plane rejected running state update for %s: %s",
                        job_id,
                        exc,
                    )
                    return
                if self._connection_is_unavailable():
                    LOG.warning(
                        "dropping running state update for %s while disconnected",
                        job_id,
                    )
                    return
            if status in {"completed", "failed", "cancelled"} and self._state_update_conflict_requires_stop(
                str(exc)
            ):
                LOG.warning(
                    "control-plane rejected terminal state update for %s: %s",
                    job_id,
                    exc,
                )
                return
            if status in {"completed", "failed", "cancelled"} and self._connection_is_unavailable():
                self._deferred_terminal_reports[job_id] = payload
                LOG.warning(
                    "deferring terminal report for %s until the worker reconnects",
                    job_id,
                )
                return
            raise

    async def _run_claimed_job(self, slot_id: str, job_payload: dict[str, Any]) -> None:
        job_id = str(job_payload["job_id"])
        request_payload = dict(job_payload.get("request") or {})
        stop_event = threading.Event()
        if worker_job_should_stop(job_payload):
            stop_event.set()
        self._active_job_stop_events[job_id] = stop_event

        loop = asyncio.get_running_loop()
        job_workspace: Path | None = None
        try:
            job_workspace = create_job_workspace(self._pipeline_base_args)
            pipeline_args = build_pipeline_namespace(
                self._pipeline_base_args,
                request_payload,
                output_dir=str(job_workspace),
            )

            async def send_running(message: str) -> None:
                await self._report_job_state(
                    job_id,
                    status="running",
                    message=message,
                )

            def emit_progress(message: str) -> None:
                future = asyncio.run_coroutine_threadsafe(send_running(message), loop)
                try:
                    future.result(timeout=5.0)
                except Exception as exc:
                    LOG.warning(
                        "failed to send progress update for %s on %s: %s",
                        job_id,
                        slot_id,
                        exc,
                    )

            await self._report_job_state(
                job_id,
                status="running",
                message="Training and validation started",
            )

            report = await asyncio.to_thread(
                run_pipeline,
                pipeline_args,
                emit_progress,
                stop_event.is_set,
            )
        except PipelineCancelledError:
            await self._report_job_state(
                job_id,
                status="cancelled",
                message="Job cancelled during pipeline execution",
            )
            return
        except Exception as exc:  # pragma: no cover - defensive runtime path
            await self._report_job_state(
                job_id,
                status="failed",
                message=str(exc),
                error=str(exc),
                traceback_text=traceback.format_exc(),
            )
            return
        finally:
            if job_workspace is not None:
                shutil.rmtree(job_workspace, ignore_errors=True)
            self._active_job_stop_events.pop(job_id, None)

        completion_message = "Training and validation completed"
        if stop_event.is_set():
            completion_message = (
                "Training completed after stop was requested; immediate interruption "
                "is not yet supported for running jobs"
            )
        await self._report_job_state(
            job_id,
            status="completed",
            message=completion_message,
            report=sanitize_pipeline_report(report),
        )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    async def _run() -> None:
        client = MlWorkerClient(args)
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(signum, client.request_shutdown)
            except (NotImplementedError, RuntimeError):  # pragma: no cover - platform/runtime dependent
                pass
        await client.run()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
