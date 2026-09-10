#!/usr/bin/env python3
"""
Runtime smoke test: does every graph operator actually get a WORKER when started?

⚠️ THIS EXISTS BECAUSE SIX NODE KINDS WERE INERT AND EVERY UNIT TEST PASSED.
`threshold`, `gap_detect` and the four marker operators were all claimed by the
start path's markers branch -- one predicate was answering both "does this
node's output carry markers" and "is this the markers node" -- so each reported
`running`, carried the markers node's status message, created no thread and
processed nothing. 129 gtest cases, ctest 3/3 and a clean build said otherwise.

The signal that distinguishes a real start from that failure is `worker_id`: the
status only carries one when a worker was actually created. A node reporting
`running` with no worker_id is the exact shape of the bug.

Needs the dev stack up and a live source stream (see natkit_synthetic_feed.py).

    natkit_operator_smoke.py --stream 909001
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import socket
import struct
import sys
import time

# Every kind that should own a worker thread, with the config it needs to start.
# `gate` and the marker operators need a marker input, so they are driven from a
# threshold rather than from the board's experiment -- which also exercises the
# operator-to-operator wiring that the "publishes nothing yet" topic resolution
# was written for.
CASES = [
    ("threshold", {"level": 0.1, "direction": "either"}),
    ("gap_detect", {"gap_ms": 200}),
    ("marker_filter", {"match_field": "event", "match_values": "rising"}),
    ("marker_debounce", {"window_ms": 50}),
]


class Ws:
    def __init__(self, host: str = "localhost", port: int = 7409) -> None:
        self.s = socket.create_connection((host, port), timeout=10)
        key = base64.b64encode(os.urandom(16)).decode()
        self.s.sendall(
            f"GET /ws/stream_viewer HTTP/1.1\r\nHost: {host}\r\n"
            f"Upgrade: websocket\r\nConnection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n".encode())
        head = self.s.recv(4096)
        if b"101" not in head.split(b"\r\n")[0]:
            raise SystemExit(f"websocket handshake failed: {head[:120]!r}")

    def send(self, obj: dict) -> None:
        p = json.dumps(obj).encode()
        m = os.urandom(4)
        n = len(p)
        h = b"\x81" + (bytes([0x80 | n]) if n < 126 else b"\xfe" + struct.pack(">H", n))
        self.s.sendall(h + m + bytes(b ^ m[i % 4] for i, b in enumerate(p)))

    def drain(self, want: bytes, timeout: float = 10.0) -> str:
        self.s.settimeout(timeout)
        buf = b""
        try:
            for _ in range(300):
                buf += self.s.recv(262144)
                if want in buf:
                    break
        except Exception:
            pass
        return buf.decode("utf-8", "replace")

    def close(self) -> None:
        self.s.close()


def node_status(blob: str, node_id: str) -> dict:
    """Pull one node's status object out of the raw frames."""
    best: dict = {}
    for m in re.finditer(r'"' + re.escape(node_id) + r'"\s*:\s*\{', blob):
        depth = 0
        start = blob.index("{", m.start())
        for i in range(start, min(start + 4000, len(blob))):
            if blob[i] == "{":
                depth += 1
            elif blob[i] == "}":
                depth -= 1
                if depth == 0:
                    try:
                        candidate = json.loads(blob[start:i + 1])
                    except Exception:
                        candidate = {}
                    # Later frames are fresher; keep the last parseable one.
                    if isinstance(candidate, dict) and "state" in candidate:
                        best = candidate
                    break
    return best


def check(ws: Ws, stream_id: str, kind: str, config: dict) -> tuple[bool, str]:
    graph_id = f"opsmoke-{kind}-{int(time.time())}"
    ident = re.sub(r"[^A-Za-z0-9]", "", graph_id)
    needs_markers = kind.startswith("marker_")

    nodes = [
        {"id": "src", "kind": "stream_source", "label": "src",
         "output_port_ids": ["data"], "position": {"x": 80, "y": 80},
         "stream_id": stream_id},
    ]
    edges = []
    if needs_markers:
        # Drive the marker operator from a threshold, so its input is a marker
        # channel that publishes nothing until the signal crosses -- which is
        # what the in-memory topic resolution exists for.
        nodes.append({"id": "thr", "kind": "threshold", "label": "thr",
                      "input_port_ids": ["in"], "output_port_ids": ["markers"],
                      "position": {"x": 320, "y": 80},
                      "output_identifier": ident + "thr",
                      "config": {"level": 0.1, "direction": "either"}})
        nodes.append({"id": "op", "kind": kind, "label": kind,
                      "input_port_ids": ["markers"], "output_port_ids": ["markers"],
                      "position": {"x": 560, "y": 80},
                      "output_identifier": ident, "config": config})
        edges = [
            {"id": "e1", "source_node_id": "src", "source_port": "data",
             "target_node_id": "thr", "target_port": "in"},
            {"id": "e2", "source_node_id": "thr", "source_port": "markers",
             "target_node_id": "op", "target_port": "markers"},
        ]
    else:
        nodes.append({"id": "op", "kind": kind, "label": kind,
                      "input_port_ids": ["in"], "output_port_ids": ["markers"],
                      "position": {"x": 320, "y": 80},
                      "output_identifier": ident, "config": config})
        edges = [{"id": "e1", "source_node_id": "src", "source_port": "data",
                  "target_node_id": "op", "target_port": "in"}]

    now = int(time.time() * 1_000_000)
    graph = {"graph_version": 1, "graph_id": graph_id, "label": f"smoke {kind}",
             "description": "", "created_at_us": now, "updated_at_us": now,
             "nodes": nodes, "edges": edges, "notes": []}

    ws.send({"action": "save_stream_graph", "graph": graph, "request_id": "s"})
    ws.drain(b"stream_graph_saved")
    ws.send({"action": "start_stream_graph", "graph_id": graph_id, "request_id": "st"})
    blob = ws.drain(b"stream_graph_started", 20)
    time.sleep(3)
    ws.send({"action": "get_stream_graph_status", "graph_id": graph_id,
             "request_id": "q"})
    blob += ws.drain(b"stream_graph_status", 10)

    status = node_status(blob, "op")
    ws.send({"action": "stop_stream_graph", "graph_id": graph_id, "request_id": "x"})
    time.sleep(0.6)
    ws.send({"action": "delete_stream_graph", "graph_id": graph_id, "force": True,
             "request_id": "d"})
    time.sleep(0.6)

    if not status:
        return False, "no status reported for the operator node"
    state = status.get("state")
    worker = status.get("worker_id")
    message = (status.get("message") or "")[:90]
    if not worker:
        return False, (f"state={state!r} but NO worker_id -- the node was claimed "
                       f"without starting. message={message!r}")
    return True, f"state={state!r} worker={worker!r}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--stream", required=True, help="a live source stream id")
    args = ap.parse_args()

    ws = Ws()
    failures = 0
    for kind, config in CASES:
        try:
            ok, detail = check(ws, args.stream, kind, config)
        except Exception as exc:  # keep going: one broken kind should not hide others
            ok, detail = False, f"raised {exc!r}"
        print(f"  {'PASS' if ok else 'FAIL'}  {kind:<16} {detail}")
        if not ok:
            failures += 1
    ws.close()

    print(f"\n  {len(CASES) - failures}/{len(CASES)} operators own a worker")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
