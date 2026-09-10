#!/usr/bin/env python3
"""
Publish synthetic sensor frames through natKit's REAL ingest path.

For exercising the graph runtime when the rig is not connected: MQTT ->
libnatkit bridge -> Kafka -> backend. Nothing here reaches into Kafka directly,
so what it proves is the same path a real sensor takes.

Written for TEC-NATKIT-106's marble-strip evidence and kept because
TEC-NATKIT-111 needs the same thing: a controllable, mixed-cadence feed that can
be started and stopped per device.

    natkit_synthetic_feed.py --device fast --seconds 600
    natkit_synthetic_feed.py --device slow --seconds 600

⚠️ RUN ONE PROCESS PER DEVICE. That is the point of --device: a test can kill
the fast feed and leave the slow one running, which is how you produce a
STARVED INPUT on demand. A single process publishing both cannot do that.

⚠️ INBOUND IS natKit/sending/, NOT natKit/receiving/. The latter is the bridge's
OUTBOUND prefix (Kafka -> device) and its own comment in KafkaMosquittoBridge
says so. Publishing there succeeds, forwards nothing, and looks exactly like a
working feed.

⚠️ ONE LONG-LIVED mosquitto_pub, fed through stdin. A `podman exec` per message
costs ~0.5 s, which capped the first version at 23 frames in 12 seconds where
~600 were wanted -- slow enough to look like a broken pipeline rather than a
slow harness.

Field names come from NatSignalFrameDataSchemaV1::decodeJson -- `channel_labels`
plus `payload` as an array of per-channel arrays. The *descriptor* declares
`channels[].samples`; the JSON wire format is the flatter pair and the decoder
is the authority.
"""
from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
import time

SCHEMA = "NatSignalFrameDataSchemaV1"

# Two deliberately different cadences. A single rate makes every marble strip
# look identical and proves nothing: the strips exist to show that two ports do
# NOT line up, so the fixture has to contain that difference.
DEVICES = {
    # ~50 Hz frames of 8 samples per channel, declared 400 Hz.
    "fast": {"device_id": 909001, "period_s": 0.020, "samples": 8,
             "rate_hz": 400, "labels": ["emg1", "emg2"]},
    # ~5 Hz frames of 4 samples per channel, declared 20 Hz.
    "slow": {"device_id": 909002, "period_s": 0.200, "samples": 4,
             "rate_hz": 20, "labels": ["accel_x", "accel_y"]},
}


class Publisher:
    """One long-lived mosquitto_pub, inside the broker's container."""

    def __init__(self, device_id: int, container: str) -> None:
        topic = f"natKit/sending/Data-{device_id}-Json-{SCHEMA}"
        self.proc = subprocess.Popen(
            ["podman", "exec", "-i", container,
             "mosquitto_pub", "-h", "localhost", "-t", topic, "-l"],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def send(self, body: str) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(body.encode() + b"\n")
        self.proc.stdin.flush()

    def close(self) -> None:
        if self.proc.stdin:
            self.proc.stdin.close()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()


def frame(spec: dict, seq: int, ts_us: int, values: list[float]) -> str:
    payload = [values, [round(v * 0.7, 4) for v in values]]
    return json.dumps({
        "schema_version": "1",
        "device_id": str(spec["device_id"]),
        "seq_no": seq,
        "device_ts_us": ts_us,
        "n_channels": len(spec["labels"]),
        "samples_per_channel": spec["samples"],
        "sample_rate_hz": spec["rate_hz"],
        "channel_labels": spec["labels"],
        "payload": payload,
    })


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--device", choices=sorted(DEVICES), required=True)
    ap.add_argument("--seconds", type=float, default=300.0)
    ap.add_argument("--container", default="mosquitto")
    # A scripted silence, for producing a gap without stopping the process.
    # Prefer killing the process for a *starved input*; use this when the same
    # run needs both a gap and a recovery.
    ap.add_argument("--gap-at", type=float, default=None,
                    help="seconds into the run to go silent")
    ap.add_argument("--gap-for", type=float, default=2.0)
    args = ap.parse_args()

    spec = DEVICES[args.device]
    pub = Publisher(spec["device_id"], args.container)

    # device_ts_us is the sensor's own clock: anchored to wall time once, then
    # advanced arithmetically, so the frames carry a clean monotonic series
    # however long each publish actually takes.
    base_us = int(time.time() * 1_000_000)
    seq = 0
    started = time.monotonic()
    next_at = 0.0

    gap_from = args.gap_at
    gap_to = None if gap_from is None else gap_from + args.gap_for
    print(f"{args.device}: device {spec['device_id']} every {spec['period_s']}s "
          f"for {args.seconds:.0f}s"
          + (f"; silent {gap_from:.0f}-{gap_to:.0f}s" if gap_from else ""),
          flush=True)

    try:
        while True:
            elapsed = time.monotonic() - started
            if elapsed >= args.seconds:
                break
            if elapsed >= next_at:
                next_at += spec["period_s"]
                silent = gap_from is not None and gap_from <= elapsed < gap_to
                if not silent:
                    phase = elapsed * 2.0 * math.pi
                    values = [round(math.sin(phase + i * 0.05), 4)
                              for i in range(spec["samples"])]
                    pub.send(frame(spec, seq,
                                   base_us + int(elapsed * 1_000_000), values))
                    seq += 1
            time.sleep(min(0.004, spec["period_s"] / 4))
    except KeyboardInterrupt:
        pass
    finally:
        pub.close()

    print(f"{args.device}: {seq} frames", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
