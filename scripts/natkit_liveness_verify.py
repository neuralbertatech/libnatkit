#!/usr/bin/env python3
"""
Does a node's reported state track whether it is actually being FED?
(TEC-NATKIT-116)

⚠️ THIS EXISTS BECAUSE THE BUG IS INVISIBLE TO UNIT TESTS AND TO SCREENSHOTS
ALIKE. `classifyGraphNodeStatusAt` is pure and exhaustively tested, but what it
is HANDED comes from a live worker's activity lanes, and the bug was never in
the arithmetic -- it was in which number the arithmetic was given. A gap
detector on a healthy stream reported `stalled` because its heartbeat was
stamped on its emit path, which a passing unit test cannot see and a screenshot
of a green board cannot disprove.

It asserts the invariant in BOTH directions, which is the whole point:

    feed running   -> a silent gap_detect/threshold reads `live`
    feed stopped   -> the same nodes read `stalled`

Checking only the first would pass just as happily against a naive per-kind
exemption ("never call these stalled"), which is the fix that reopens
TEC-NATKIT-123 -- a node reporting health forever because nobody ever asks
whether anything is still arriving. The second assertion is what separates the
two implementations, so it is the one that matters.

The threshold is configured with a level far above the synthetic signal, so it
NEVER fires: a permanently silent operator on a permanently healthy stream, the
case that used to mark a whole board unhealthy.

Needs the dev stack up. Manages the synthetic feed itself, so a run is
repeatable and the "stopped" half is real rather than simulated.

    natkit_liveness_verify.py
    natkit_liveness_verify.py --keep-board   # leave it up to screenshot
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import signal
import socket
import struct
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
FEED = os.path.join(HERE, "natkit_synthetic_feed.py")

# The `fast` device in natkit_synthetic_feed.py.
STREAM_ID = "909001"

# The classifier's window is 3 s; everything here is given a margin on top of it
# so a slow poll or a scheduling hiccup cannot decide the result.
STALL_WINDOW_S = 3.0
SETTLE_S = 6.0


class Ws:
    """Minimal client for /ws/stream_viewer -- same shape as the other scripts."""

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
    """The freshest status object for one node, out of the raw frames."""
    best: dict = {}
    for m in re.finditer(r'"' + re.escape(node_id) + r'"\s*:\s*\{', blob):
        depth = 0
        start = blob.index("{", m.start())
        for i in range(start, min(start + 8000, len(blob))):
            if blob[i] == "{":
                depth += 1
            elif blob[i] == "}":
                depth -= 1
                if depth == 0:
                    try:
                        candidate = json.loads(blob[start:i + 1])
                    except Exception:
                        candidate = {}
                    if isinstance(candidate, dict) and "state" in candidate:
                        best = candidate  # later frames are fresher
                    break
    return best


def run_state(blob: str) -> str:
    """The board's derived run_state, from the last status frame that carries one."""
    found = re.findall(r'"run_state"\s*:\s*"([a-z_]+)"', blob)
    return found[-1] if found else "?"


def build_graph(graph_id: str) -> dict:
    ident = re.sub(r"[^A-Za-z0-9]", "", graph_id)
    nodes = [
        {"id": "src", "kind": "stream_source", "label": "Fast 400Hz",
         "output_port_ids": ["data"], "position": {"x": 80, "y": 120},
         "stream_id": STREAM_ID},
        {"id": "gap", "kind": "gap_detect", "label": "Gap detector",
         "input_port_ids": ["in"], "output_port_ids": ["markers"],
         "position": {"x": 360, "y": 40},
         "output_identifier": ident + "gap",
         # Far longer than any gap the healthy feed produces, so it stays silent.
         "config": {"gap_ms": 2000}},
        {"id": "thr", "kind": "threshold", "label": "Threshold (never fires)",
         "input_port_ids": ["in"], "output_port_ids": ["markers"],
         "position": {"x": 360, "y": 220},
         "output_identifier": ident + "thr",
         # ⚠️ 1000.0 is far above anything the synthetic signal reaches, ON
         # PURPOSE: a threshold that never crosses is a permanently silent
         # operator on a permanently healthy stream.
         "config": {"level": 1000.0, "direction": "either"}},
    ]
    edges = [
        {"id": "e1", "source_node_id": "src", "source_port": "data",
         "target_node_id": "gap", "target_port": "in"},
        {"id": "e2", "source_node_id": "src", "source_port": "data",
         "target_node_id": "thr", "target_port": "in"},
    ]
    now = int(time.time() * 1_000_000)
    return {"graph_version": 1, "graph_id": graph_id,
            "label": "TEC-NATKIT-116 liveness", "description": "",
            "created_at_us": now, "updated_at_us": now,
            "nodes": nodes, "edges": edges, "notes": []}


def poll(ws: Ws, graph_id: str) -> tuple[dict, dict, str]:
    ws.send({"action": "get_stream_graph_status", "graph_id": graph_id,
             "request_id": "q"})
    blob = ws.drain(b"stream_graph_status", 10)
    return node_status(blob, "gap"), node_status(blob, "thr"), run_state(blob)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--keep-board", action="store_true",
                    help="leave the board running and saved, to screenshot")
    args = ap.parse_args()

    graph_id = f"liveness-116-{int(time.time())}"
    feed = subprocess.Popen(
        [sys.executable, FEED, "--device", "fast", "--seconds", "600"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    failures: list[str] = []

    try:
        print(f"  feed up (pid {feed.pid}), letting the stream establish...")
        time.sleep(5)

        ws = Ws()
        ws.send({"action": "save_stream_graph", "graph": build_graph(graph_id),
                 "request_id": "s"})
        ws.drain(b"stream_graph_saved")
        ws.send({"action": "start_stream_graph", "graph_id": graph_id,
                 "request_id": "st"})
        ws.drain(b"stream_graph_started", 20)

        # Past the stall window, so "live" here cannot be the startup grace.
        print(f"  running; waiting {SETTLE_S:.0f}s (past the {STALL_WINDOW_S:.0f}s "
              f"stall window) so `live` cannot be the startup grace...")
        time.sleep(SETTLE_S)
        gap, thr, board = poll(ws, graph_id)

        print(f"\n  FED   gap_detect state={gap.get('state')!r} "
              f"frames_processed={gap.get('frames_processed')} "
              f"worker={bool(gap.get('worker_id'))}")
        print(f"  FED   threshold  state={thr.get('state')!r} "
              f"frames_processed={thr.get('frames_processed')} "
              f"worker={bool(thr.get('worker_id'))}")
        print(f"  FED   board run_state={board!r}")

        # ⚠️ frames_processed counts EMISSIONS. Both nodes must read 0 here, or
        # they were not actually silent and the test proved nothing.
        for name, status in (("gap_detect", gap), ("threshold", thr)):
            if not status:
                failures.append(f"{name}: no status reported at all")
                continue
            if not status.get("worker_id"):
                failures.append(f"{name}: reported without a worker_id")
            if status.get("frames_processed", 0) != 0:
                failures.append(
                    f"{name}: emitted {status.get('frames_processed')} -- it was "
                    "NOT silent, so this run does not test what it claims")
            if status.get("state") != "live":
                failures.append(
                    f"{name}: silent on a healthy feed but reads "
                    f"{status.get('state')!r}, want 'live'")
        if board == "stalled":
            failures.append(
                "board run_state is 'stalled' while its feed is healthy -- one "
                "silent operator is still poisoning the whole graph")

        # The other direction: kill the feed and the SAME nodes must go stalled.
        print("\n  stopping the feed...")
        feed.send_signal(signal.SIGTERM)
        try:
            feed.wait(timeout=10)
        except subprocess.TimeoutExpired:
            feed.kill()
        time.sleep(SETTLE_S)
        gap, thr, board = poll(ws, graph_id)

        print(f"\n  DEAD  gap_detect state={gap.get('state')!r}")
        print(f"  DEAD  threshold  state={thr.get('state')!r}")
        print(f"  DEAD  board run_state={board!r}")

        for name, status in (("gap_detect", gap), ("threshold", thr)):
            if status.get("state") != "stalled":
                failures.append(
                    f"{name}: upstream is dead but reads {status.get('state')!r}, "
                    "want 'stalled' -- a per-kind exemption would fail exactly "
                    "here (TEC-NATKIT-123 again)")

        if not args.keep_board:
            ws.send({"action": "stop_stream_graph", "graph_id": graph_id,
                     "request_id": "x"})
            time.sleep(0.6)
            ws.send({"action": "delete_stream_graph", "graph_id": graph_id,
                     "force": True, "request_id": "d"})
            time.sleep(0.6)
        else:
            print(f"\n  board left up: {graph_id}")
        ws.close()
    finally:
        if feed.poll() is None:
            feed.kill()

    print()
    for line in failures:
        print(f"  FAIL  {line}")
    if failures:
        return 1
    print("  PASS  silent-but-fed reads live; not-fed reads stalled, both kinds")
    return 0


if __name__ == "__main__":
    sys.exit(main())
