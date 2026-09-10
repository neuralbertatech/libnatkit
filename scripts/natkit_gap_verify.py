#!/usr/bin/env python3
"""
End-to-end check for TEC-NATKIT-115: does a gap_detect node actually publish a
correct marker when a real feed stops?

The unit tests cover the detection logic. What they cannot cover is the rest of
the path: the node starting, the marker reaching a real Kafka MARKER topic, and
the marker's CONTENT being right. A screenshot cannot check content, so this
reads the marker back and asserts its fields.

Assumes both synthetic feeds are running, one process per device.

Two traps this script was wrong about before it was right, both of which make a
CORRECT marker look like a missing one:

  * `kafka-console-consumer --from-beginning` read 0 records from the live
    marker topic while `kafka-log-dirs` showed 432 bytes on disk. An explicit
    `--partition 0 --offset 0` reads it.
  * The marker's fields live in a nested `attributes` OBJECT. Reading
    `attributes_json` yields nothing, so every assertion below passes on {}.

    natkit_gap_verify.py
"""
import base64
import json
import os
import re
import struct
import socket
import subprocess
import sys
import time

WS_HOST, WS_PORT = "localhost", 7409
FAST_ID = "909001"
GRAPH_ID = f"gapcheck-{int(time.time())}"
GAP_MS = 300


class Ws:
    def __init__(self) -> None:
        self.s = socket.create_connection((WS_HOST, WS_PORT), timeout=10)
        key = base64.b64encode(os.urandom(16)).decode()
        self.s.sendall(
            f"GET /ws/stream_viewer HTTP/1.1\r\nHost: {WS_HOST}\r\n"
            f"Upgrade: websocket\r\nConnection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n".encode())
        head = self.s.recv(4096)
        assert b"101" in head.split(b"\r\n")[0], head[:120]

    def send(self, obj: dict) -> None:
        p = json.dumps(obj).encode()
        m = os.urandom(4)
        n = len(p)
        h = b"\x81" + (bytes([0x80 | n]) if n < 126 else b"\xfe" + struct.pack(">H", n))
        self.s.sendall(h + m + bytes(b ^ m[i % 4] for i, b in enumerate(p)))

    def drain(self, want: bytes, timeout: float = 8.0) -> str:
        self.s.settimeout(timeout)
        buf = b""
        try:
            for _ in range(200):
                buf += self.s.recv(262144)
                if want in buf:
                    break
        except Exception:
            pass
        return buf.decode("utf-8", "replace")

    def close(self) -> None:
        self.s.close()


def kafka(*args: str) -> str:
    return subprocess.run(
        ["podman", "exec", "natkit_natkit-v0-kafka_1", "bash", "-lc", " ".join(args)],
        capture_output=True, text=True).stdout


def main() -> int:
    ws = Ws()

    graph = {
        "graph_version": 1,
        "graph_id": GRAPH_ID,
        "label": "gap check",
        "description": "",
        "created_at_us": int(time.time() * 1_000_000),
        "updated_at_us": int(time.time() * 1_000_000),
        "nodes": [
            {"id": "src", "kind": "stream_source", "label": "fast",
             "output_port_ids": ["data"], "position": {"x": 100, "y": 100},
             "stream_id": FAST_ID},
            {"id": "gap", "kind": "gap_detect", "label": "Gap detector",
             "input_port_ids": ["in"], "output_port_ids": ["markers"],
             "position": {"x": 400, "y": 100},
             "output_identifier": GRAPH_ID.replace("-", ""),
             "config": {"gap_ms": GAP_MS}},
        ],
        "edges": [
            {"id": "e1", "source_node_id": "src", "source_port": "data",
             "target_node_id": "gap", "target_port": "in"},
        ],
        "notes": [],
    }

    ws.send({"action": "save_stream_graph", "graph": graph, "request_id": "save"})
    ws.drain(b"stream_graph_saved", 10)

    ws.send({"action": "validate_stream_graph", "graph_id": GRAPH_ID, "request_id": "v"})
    val = ws.drain(b"stream_graph_validation", 10)
    if '"does not exist"' in val or '"severity":"error"' in val:
        problems = re.findall(r'"message"\s*:\s*"([^"]{0,140})"', val)
        print("  VALIDATION PROBLEMS:", problems[:6])
        return 1
    print("  validated clean")

    # ⚠️ THE GAP MUST RECOVER. The detector compares a new frame against where
    # it was expected, so it fires when data RESUMES -- a dropout that never
    # recovers produces no marker at all (that case is a stalled node, by
    # design: see TEC-NATKIT-110). An earlier version of this check killed the
    # feed and waited, which can never pass.
    subprocess.run(["pkill", "-f", "natkit_synthetic_feed.py --device fast"],
                   capture_output=True)
    time.sleep(1)
    feeder = subprocess.Popen(
        [sys.executable, "libnatkit/scripts/natkit_synthetic_feed.py",
         "--device", "fast", "--seconds", "60",
         "--gap-at", "12", "--gap-for", "3"],
        cwd="/home/zach/code/natKit",
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("  feed restarted with a scripted 3s silence at t+12s")
    time.sleep(2)

    ws.send({"action": "start_stream_graph", "graph_id": GRAPH_ID, "request_id": "st"})
    started = ws.drain(b"stream_graph_started", 20)
    if '"state":"error"' in started:
        print("  START ERROR:", re.findall(r'"message"\s*:\s*"([^"]{0,160})"', started)[:4])
        return 1
    print(f"  started; waiting through the scripted silence for a gap > {GAP_MS}ms")
    time.sleep(22)

    # ⚠️ The output identifier is HASHED into the stream id (stableStreamId),
    # not embedded in the topic name -- so searching the topic list for the
    # identifier can never match. Ask the graph status for the id instead.
    ws.send({"action": "get_stream_graph_status", "graph_id": GRAPH_ID,
             "request_id": "q"})
    status_blob = ws.drain(b"stream_graph_status", 10)
    ids = re.findall(r'"output_stream_id"\s*:\s*"(\d+)"', status_blob)
    topics = kafka("kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null")
    marker_topic = next(
        (t for t in topics.split()
         if t.startswith("Marker-") and any(i in t for i in ids)),
        None)
    if marker_topic is None:
        print("  NO MARKER TOPIC. node output_stream_ids:", ids)
        print("  Marker topics present:",
              [t for t in topics.split() if t.startswith("Marker-")][:6])
        ws.send({"action": "stop_stream_graph", "graph_id": GRAPH_ID, "request_id": "x"})
        return 1
    print(f"  marker topic: {marker_topic}")

    # ⚠️ --from-beginning reads 0 records from a LIVE topic here; an explicit
    # --partition/--offset does not. The topic had 432 bytes on disk while
    # the consumer reported nothing, which reads as "no markers published".
    out = kafka(f"timeout 25 kafka-console-consumer --bootstrap-server localhost:9092 "
                f"--topic {marker_topic} --partition 0 --offset 0 "
                f"--max-messages 5 --timeout-ms 15000 2>/dev/null")
    records = [json.loads(l) for l in out.splitlines() if l.strip().startswith("{")]
    print(f"  markers read back: {len(records)}")
    ok = True
    for rec in records[:3]:
        # MarkerEventV1 carries a nested `attributes` object on the wire.
        # An earlier version read "attributes_json" -- absent, so every
        # field read as None and the checks below passed on nothing.
        attrs = rec.get("attributes") or {}
        if not attrs:
            print("    ^ NO attributes on the marker"); ok = False
        print(f"    event={rec.get('event')!r} type={rec.get('marker_type')!r} "
              f"at={rec.get('emitted_at_us')} gap_us={attrs.get('gap_us')} "
              f"frames_lost={attrs.get('frames_lost')} "
              f"seq={attrs.get('seq_before')}->{attrs.get('seq_after')}")
        if rec.get("event") not in ("gap_lost", "gap_paused"):
            print("    ^ UNEXPECTED event value"); ok = False
        if not attrs.get("gap_us") or attrs["gap_us"] < GAP_MS * 1000:
            print("    ^ gap_us below the configured threshold"); ok = False

    ws.send({"action": "stop_stream_graph", "graph_id": GRAPH_ID, "request_id": "x"})
    time.sleep(1)
    ws.send({"action": "delete_stream_graph", "graph_id": GRAPH_ID,
             "force": True, "request_id": "d"})
    time.sleep(1)
    ws.close()
    feeder.terminate()

    if not records:
        print("  FAIL: the feed stopped and no gap marker was published")
        return 1
    print("  PASS" if ok else "  FAIL: a marker's contents were wrong")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
