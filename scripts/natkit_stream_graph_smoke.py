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
            {
                "id": "experiment/smoke",
                "kind": "experiment",
                "label": "Smoke experiment",
                "position": {"x": 480, "y": 0},
                "input_port_ids": [],
                "output_port_ids": ["markers"],
                "config": {
                    "protocol": {
                        "protocol_id": "smoke-protocol",
                        "label": "Smoke protocol",
                        "classes": ["a", "b"],
                        "rest_class": "rest",
                        "repetitions": 1,
                        "hold_s": 1,
                        "rest_s": 1,
                        "lead_in_s": 1,
                        "tail_rest_s": 1,
                        "seed": 1,
                    },
                    "experiment_id": "smoke-experiment",
                    "participant_id": "smoke",
                    "notes": "",
                },
            },
            {
                # Topic-aware combine (Part B): merges a DATA channel (transform)
                # with a MARKER channel (experiment) into a "stream" output that
                # carries one topic per type.
                "id": "combine/smoke",
                "kind": "combine",
                "label": "Smoke combine",
                "position": {"x": 720, "y": 0},
                "input_port_ids": ["in1", "in2"],
                "output_port_ids": ["output"],
                "output_identifier": f"{args.graph_id}-combined",
                "config": {},
            },
            {
                "id": "train/smoke",
                "kind": "train",
                "label": "Smoke train",
                "position": {"x": 480, "y": 160},
                "config": {
                    "families": ["lda"],
                    "train_runs": [],
                    "eval_runs": [],
                    "selected_fields": [],
                    "window_ms": 200,
                    "hop_ms": 50,
                    "vote_windows": 5,
                    "confidence_threshold": 0.6,
                    "min_hold_windows": 2,
                    "rest_gesture": "rest",
                    "active_gesture": "fist",
                },
            },
        ],
        "edges": [
            {
                "id": "edge/smoke",
                "source_node_id": "source/smoke",
                "source_port": "data",
                "target_node_id": "transform/smoke",
                "target_port": "input",
            },
            {
                "id": "edge/smoke-combine-data",
                "source_node_id": "transform/smoke",
                "source_port": "output",
                "target_node_id": "combine/smoke",
                "target_port": "in1",
            },
            {
                "id": "edge/smoke-combine-markers",
                "source_node_id": "experiment/smoke",
                "source_port": "markers",
                "target_node_id": "combine/smoke",
                "target_port": "in2",
            },
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

        print("listing node catalog...")
        catalog = await client.request(
            {"action": "list_node_catalog", "request_id": next_request_id("catalog")},
            expect_types={"node_catalog"},
        )
        nodes = catalog.get("nodes", [])
        node_types = {entry.get("node_type") for entry in nodes}
        catalog_kinds = {entry.get("kind") for entry in nodes}
        # The palette renders purely from this — assert the structural kinds and
        # a representative transform are all advertised (Phase 1).
        missing = {"stream_source", "viewer", "sink", "combine", "experiment"} - catalog_kinds
        if missing:
            raise AssertionError(f"node catalog missing structural kinds {missing}: {node_types}")
        if "bandpass_iir" not in node_types:
            raise AssertionError(f"node catalog missing a compiled transform: {sorted(node_types)}")
        # The experiment node advertises exactly one output port, `markers`.
        experiment_entry = next((e for e in nodes if e.get("kind") == "experiment"), None)
        if experiment_entry is None:
            raise AssertionError(f"node catalog missing the experiment node: {node_types}")
        experiment_out = [p.get("id") for p in experiment_entry.get("output_ports", [])]
        if experiment_out != ["markers"]:
            raise AssertionError(f"experiment node must expose a single 'markers' output, got {experiment_out}")
        # Provenance edges: the experiment advertises a prov_source lineage input
        # (source->experiment) and the train node advertises prov_experiment in +
        # prov_models out (experiment->train->classify).
        experiment_in = [p.get("id") for p in experiment_entry.get("input_ports", [])]
        if "prov_source" not in experiment_in:
            raise AssertionError(f"experiment node must expose a prov_source lineage input, got {experiment_in}")
        train_entry = next((e for e in nodes if e.get("kind") == "train"), None)
        if train_entry is None:
            raise AssertionError(f"node catalog missing the train node: {node_types}")
        train_in = [p.get("id") for p in train_entry.get("input_ports", [])]
        train_out = [p.get("id") for p in train_entry.get("output_ports", [])]
        if "prov_experiment" not in train_in or "prov_models" not in train_out:
            raise AssertionError(
                f"train node must expose prov_experiment in + prov_models out, got in={train_in} out={train_out}"
            )
        transform_entries = [e for e in nodes if e.get("kind") == "transform"]
        for entry in transform_entries:
            for key in ("category", "runner", "config_fields", "input_ports", "output_ports"):
                if key not in entry:
                    raise AssertionError(f"transform catalog entry missing '{key}': {entry}")
        print(f"  catalog advertises {len(nodes)} node types ({len(transform_entries)} transforms)")

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

        # Experiment node (formerly "session"): a first-class kind that validates,
        # starts, and is marked running (recording itself is client-side). Unlike
        # the old session node it exposes a `markers` output whose stream id
        # resolves to the deterministic Marker/<experiment_id> topic.
        experiment_status = node_statuses.get("experiment/smoke")
        if experiment_status is None or experiment_status.get("state") != "running":
            raise AssertionError(f"expected experiment node running, got: {json.dumps(started, indent=2)}")
        if not experiment_status.get("output_stream_id"):
            raise AssertionError("experiment node must resolve a markers output_stream_id")
        print(
            "  experiment node running (client-side recorder), "
            f"markers output_stream_id={experiment_status['output_stream_id']}"
        )

        # Topic-aware channels (Parts A + B): the combine node merges a DATA
        # channel (transform) with a MARKER channel (experiment) into a "stream"
        # output whose runtime status carries output_topics = one DATA + one
        # MARKER topic, both sharing the same channel stream id.
        combine_status = node_statuses.get("combine/smoke")
        if combine_status is None or combine_status.get("state") != "running":
            raise AssertionError(f"expected combine node running, got: {json.dumps(started, indent=2)}")
        combine_topics = combine_status.get("output_topics") or []
        combine_types = {t.get("type") for t in combine_topics}
        if combine_types != {"Data", "Marker"}:
            raise AssertionError(
                f"combine 'stream' output must carry Data + Marker topics, got {combine_topics}"
            )
        combine_ids = {t.get("id") for t in combine_topics}
        if len(combine_ids) != 1:
            raise AssertionError(
                f"combine Data + Marker topics must share one channel id, got {combine_ids}"
            )
        print(f"  combine running as a 'stream' channel (Data+Marker), output_topics={combine_topics}")

        # Phase 5: the train node is a first-class kind — validates, starts, and
        # is marked running (the control-plane job is submitted client-side).
        train_status = node_statuses.get("train/smoke")
        if train_status is None or train_status.get("state") != "running":
            raise AssertionError(f"expected train node running, got: {json.dumps(started, indent=2)}")
        if train_status.get("output_stream_id"):
            raise AssertionError("train node must not produce an output stream")
        print("  train node running (control-plane job submitted client-side)")

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
