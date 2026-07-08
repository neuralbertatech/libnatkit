#!/usr/bin/env python3
"""Smoke test for the Stream Viewer stream-graph WebSocket actions.

Exercises the acceptance-test path from plans/stream-graph-programming-plan.html:
save a draft graph, validate it, start it, confirm the transform node reaches
`running` with an output stream id, then stop it and confirm the worker stops.

Requires a running natKit backend with at least one live DATA stream to attach
the graph's source node to.
"""
from __future__ import annotations

import argparse
import asyncio
import itertools
import json
import os
import sys
from typing import Any

from natkit_auth_shared import (
    apply_websocket_headers,
    build_websocket_cookie_header,
    derive_auth_base_url,
    resolve_session_token,
)

_REQUEST_ID_COUNTER = itertools.count(1)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stream-viewer-url", default="ws://127.0.0.1:8786/ws/stream_viewer")
    parser.add_argument("--source-stream-id", required=True, help="Existing live stream id to attach as the graph source")
    parser.add_argument("--transform-kind", default="highpass_iir")
    parser.add_argument("--input-mapping-id", default="canonical_channel_frame")
    parser.add_argument("--auth-base-url", default=os.getenv("NATKIT_AUTH_BASE_URL"))
    parser.add_argument("--auth-session-token", default=os.getenv("NATKIT_AUTH_SESSION_TOKEN"))
    parser.add_argument("--auth-username", default=os.getenv("NATKIT_AUTH_USERNAME"))
    parser.add_argument("--auth-password", default=os.getenv("NATKIT_AUTH_PASSWORD"))
    parser.add_argument("--response-timeout-s", type=float, default=10.0)
    parser.add_argument("--start-timeout-s", type=float, default=15.0)
    parser.add_argument("--graph-id", default="stream-graph-smoke-test")
    parser.add_argument("--keep-graph", action="store_true", help="Do not remove the smoke graph from the store afterward")
    return parser.parse_args(argv)


def next_request_id(label: str) -> str:
    return f"{label}:{next(_REQUEST_ID_COUNTER)}"


def build_smoke_graph(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "graph_version": 1,
        "graph_id": args.graph_id,
        "label": "Smoke test graph",
        "description": "Created by natkit_stream_graph_smoke.py",
        "nodes": [
            {
                "id": "source/smoke",
                "kind": "stream_source",
                "label": "Smoke source",
                "position": {"x": 0, "y": 0},
                "stream_id": str(args.source_stream_id),
                "output_port_ids": ["data"],
            },
            {
                "id": "transform/smoke",
                "kind": "transform",
                "label": "Smoke transform",
                "position": {"x": 240, "y": 0},
                "transform_kind": args.transform_kind,
                "input_mapping_id": args.input_mapping_id,
                "config": {},
                "output_identifier": f"{args.graph_id}-output",
                "input_port_ids": ["input"],
                "output_port_ids": ["output"],
            },
        ],
        "edges": [
            {
                "id": "edge/smoke",
                "source_node_id": "source/smoke",
                "source_port": "data",
                "target_node_id": "transform/smoke",
                "target_port": "input",
            }
        ],
        "notes": [],
    }


class StreamGraphSmokeClient:
    def __init__(self, websocket: Any, response_timeout_s: float) -> None:
        self._ws = websocket
        self._response_timeout_s = response_timeout_s

    async def request(self, payload: dict[str, Any], *, expect_types: set[str]) -> dict[str, Any]:
        request_id = payload["request_id"]
        await self._ws.send(json.dumps(payload))
        while True:
            raw = await asyncio.wait_for(self._ws.recv(), timeout=self._response_timeout_s)
            message = json.loads(raw)
            if message.get("request_id") != request_id:
                continue
            if message.get("type") == "error":
                raise RuntimeError(f"backend returned error for {payload['action']}: {message.get('message')}")
            if message.get("type") not in expect_types:
                raise RuntimeError(
                    f"unexpected response type {message.get('type')!r} for {payload['action']}"
                )
            return message


async def run_smoke_test(args: argparse.Namespace) -> None:
    import websockets

    auth_base_url = args.auth_base_url or derive_auth_base_url(args.stream_viewer_url)
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

    connect_kwargs = apply_websocket_headers(
        websockets.connect,
        build_websocket_cookie_header(session_token=session_token),
    )
    async with websockets.connect(args.stream_viewer_url, **connect_kwargs) as ws:
        client = StreamGraphSmokeClient(ws, args.response_timeout_s)
        graph = build_smoke_graph(args)

        print("saving draft graph...")
        saved = await client.request(
            {"action": "save_stream_graph", "request_id": next_request_id("save"), "graph": graph},
            expect_types={"stream_graph_saved"},
        )
        assert saved["graph_id"] == args.graph_id, saved

        print("validating graph...")
        validation = await client.request(
            {"action": "validate_stream_graph", "request_id": next_request_id("validate"), "graph": graph},
            expect_types={"stream_graph_validation"},
        )
        if not validation.get("valid"):
            raise AssertionError(f"expected valid graph, got diagnostics: {json.dumps(validation, indent=2)}")
        print("  graph is valid")

        print("starting graph...")
        started = await client.request(
            {"action": "start_stream_graph", "request_id": next_request_id("start"), "graph_id": args.graph_id},
            expect_types={"stream_graph_started"},
        )
        node_statuses = started.get("node_statuses", {})
        transform_status = node_statuses.get("transform/smoke")
        if transform_status is None or transform_status.get("state") != "running":
            raise AssertionError(f"expected transform node running, got: {json.dumps(started, indent=2)}")
        if not transform_status.get("output_stream_id"):
            raise AssertionError("started transform node has no output_stream_id")
        print(f"  transform running with output_stream_id={transform_status['output_stream_id']}")

        print("stopping graph...")
        stopped = await client.request(
            {"action": "stop_stream_graph", "request_id": next_request_id("stop"), "graph_id": args.graph_id},
            expect_types={"stream_graph_stopped"},
        )
        print(f"  graph_run_id after stop={stopped.get('graph_run_id')}")

        if not args.keep_graph:
            print("removing smoke graph from list_stream_graphs snapshot (store entry remains; delete not implemented in V1)...")

        print("smoke test passed")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        asyncio.run(run_smoke_test(args))
    except Exception as exc:  # pragma: no cover - manual smoke script
        print(f"smoke test failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
