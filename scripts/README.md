# libnatkit scripts

This directory holds backend and integration scripts that belong to core
natKit infrastructure rather than a client-specific project.

## ML control plane

`natkit_ml_control_plane.py` hosts the WebSocket control plane used by the
frontend ML workspace. It owns the transport layer and job orchestration
surface. The current EMG train/validate implementation it calls is still in the
`natVR` package until that runtime is generalized or moved into `libnatkit`.

It now also exposes the first worker-slot scheduling surface for natKit. The
service creates a logical worker node with a slot count that defaults to the
host thread count unless `--worker-threads` is supplied. Jobs are queued onto a
selected slot with priority and streamed back to the frontend as scheduler
state. The current protocol also exposes `list_workers`, which reports the
embedded worker's slot-capacity summary and a heartbeat timestamp. That keeps
the UI contract stable while the runtime is still single-process. Job update
and stop actions are also owner-scoped using the current placeholder principal
field so separate browser sessions do not mutate each other's queued work by
default.

For local durability without introducing a full database yet, the service also
accepts `--state-json <path>` and `--state-db <path>`. The JSON snapshot is a
simple export, while the SQLite database is the stronger local state path for
slot ownership and job history across control-plane restarts. Any non-terminal
job recovered from either backend is marked failed on startup because in-flight
execution cannot be resumed.

Run it with the natVR virtual environment so the current pipeline dependencies
are available:

```sh
source natVR/.venv/bin/activate
python libnatkit/scripts/natkit_ml_control_plane.py \
  --broker 127.0.0.1:29092
```

The frontend ML workspace expects this service at `ws://127.0.0.1:8786` by
default and is available at `#/MlPipeline` in the frontend app. The control
plane uses a per-job scratch workspace and removes those temporary files after
the job finishes.

When you run the local dev stack through
`podman compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build`,
the override now also starts a dedicated `natkit-v0-ml-control-plane` service
on host port `8786` with a persisted SQLite scheduler state under
`uploads/ml-control-plane/`. The compose service now forwards
`NATKIT_ML_CONTROL_PLANE_BROKER`,
`NATKIT_ML_CONTROL_PLANE_WORKER_THREADS`, and
`NATKIT_ML_CONTROL_PLANE_MAX_WORKER_RESTART_ATTEMPTS`
into that container so the image's runtime overrides actually take effect.

An opt-in standalone worker service is also available through the
`ml-worker` compose profile:

```sh
podman compose -f docker-compose.yml -f docker-compose.dev.yml \
  --profile ml-worker up -d --build
```

That profile starts `natkit-v0-ml-worker-a`, which connects to the control
plane over the internal compose network, mirrors four logical slots, and uses
`uploads/ml-worker-a/` for scratch space. You can override that profile's
control-plane URL, worker id, or slot count with
`NATKIT_ML_WORKER_CONTROL_PLANE_URL`, `NATKIT_ML_WORKER_ID_A`, and
`NATKIT_ML_WORKER_THREADS_A`.

If you want the control plane to run without any embedded execution slots so
that only external workers can accept jobs, set
`NATKIT_ML_CONTROL_PLANE_WORKER_THREADS=0` when starting the stack:

```sh
NATKIT_ML_CONTROL_PLANE_WORKER_THREADS=0 \
podman compose -f docker-compose.yml -f docker-compose.dev.yml \
  --profile ml-worker up -d --build
```

## Standalone ML worker

`natkit_ml_worker.py` is the first separate worker runtime for that scheduler
surface. It connects to the control plane over WebSocket, registers an external
worker identity, mirrors its logical slot inventory, claims queued jobs for its
slots, and reports running or terminal job state back to the control plane.

Run it from the same environment as the control plane:

```sh
source natVR/.venv/bin/activate
python libnatkit/scripts/natkit_ml_worker.py \
  --control-plane-url ws://127.0.0.1:8786 \
  --worker-id worker-remote-a \
  --worker-threads 8
```

The worker uses the same temporary scratch-workspace model as the embedded
runner. It now retries control-plane connectivity with backoff and replays any
deferred terminal job-state reports after reconnect, and it now reasserts any
still-running claimed jobs back to the control plane after reconnect so restart
recovery does not stay in a placeholder running state indefinitely. Current
remote workers now also advertise a per-process worker session id so the
control plane can tell a normal reconnect from a true worker-process restart;
if a new worker process comes back under the same worker id, any in-flight jobs
for that worker are requeued and rerun from the start. The scheduler now persists
and reports a per-job restart count so those reruns are visible in API payloads
and the ML Pipeline UI, and worker plus slot summaries now include cumulative
restart-attempt counts for a quick instability view. Automatic reruns are now
bounded by `--max-worker-restart-attempts` on the control plane, or by
`NATKIT_ML_CONTROL_PLANE_MAX_WORKER_RESTART_ATTEMPTS` in the containerized dev
stack. When that limit is exhausted, the job is marked failed and the existing
ML Pipeline job-status and error-banner flow becomes the user-visible
notification path. Current hardening gaps are
deployment topology, richer recovery for in-flight work across control-plane
restarts, and moving scheduler durability beyond the control plane's local JSON
and SQLite state.

## Worker operator CLI

`natkit_ml_worker_operator.py` is a small operator-facing wrapper around the ML
control-plane WebSocket protocol for worker inspection plus drain and resume
flows. It is meant for deployment and maintenance scripts that should not
hand-craft raw WebSocket messages.

List worker summaries:

```sh
source natVR/.venv/bin/activate
python libnatkit/scripts/natkit_ml_worker_operator.py list
```

Inspect one worker:

```sh
python libnatkit/scripts/natkit_ml_worker_operator.py \
  status \
  --worker-id worker-remote-a
```

Inspect multiple workers in one call:

```sh
python libnatkit/scripts/natkit_ml_worker_operator.py \
  status \
  --worker-id worker-remote-a \
  --worker-id worker-remote-b
```

Load worker ids from a file:

```sh
python libnatkit/scripts/natkit_ml_worker_operator.py \
  status \
  --workers-file workers.txt
```

Load worker ids from a structured JSON manifest:

```sh
python libnatkit/scripts/natkit_ml_worker_operator.py \
  status \
  --workers-json workers.json
```

Filter JSON manifest entries by group:

```sh
python libnatkit/scripts/natkit_ml_worker_operator.py \
  drain \
  --workers-json workers.json \
  --worker-group blue
```

Drain a worker and wait until it is ready to stop:

```sh
source natVR/.venv/bin/activate
python libnatkit/scripts/natkit_ml_worker_operator.py \
  drain \
  --control-plane-url ws://127.0.0.1:8786 \
  --worker-id worker-remote-a
```

Drain a worker without waiting:

```sh
python libnatkit/scripts/natkit_ml_worker_operator.py \
  drain \
  --worker-id worker-remote-a \
  --no-wait
```

Resume a worker so it can accept new jobs again:

```sh
python libnatkit/scripts/natkit_ml_worker_operator.py \
  resume \
  --worker-id worker-remote-a
```

Drain or resume multiple workers by repeating `--worker-id`:

```sh
python libnatkit/scripts/natkit_ml_worker_operator.py \
  drain \
  --worker-id worker-remote-a \
  --worker-id worker-remote-b
```

The `--workers-file` input is available for `status`, `drain`, and `resume`.
It expects a newline-delimited file of worker ids, ignores blank lines and
lines starting with `#`, and combines with repeated `--worker-id` entries.
When both are supplied, the CLI preserves first-seen order and removes
duplicates before issuing maintenance actions.

The `--workers-json` input is also available for `status`, `drain`, and
`resume`. It accepts either a JSON array such as
`["worker-a", "worker-b"]` or an object like
`{"workers": ["worker-a", {"worker_id": "worker-b", "group": "blue"}]}`.
When `--worker-group` is supplied, only object entries whose `group` or
`groups` field matches are selected. String entries do not participate in group
filtering.

Stop after the first hard request error during a coordinated run:

```sh
python libnatkit/scripts/natkit_ml_worker_operator.py \
  drain \
  --worker-id worker-remote-a \
  --worker-id worker-remote-b \
  --fail-fast
```

Write a structured result artifact for deployment automation:

```sh
python libnatkit/scripts/natkit_ml_worker_operator.py \
  drain \
  --workers-json workers.json \
  --worker-group blue \
  --result-json-out build/worker-drain-result.json
```

The `list` and `status` commands use `list_workers` under the hood. The
`drain` command uses the control plane's `update_worker_status` and
`wait_worker_drain_ready` actions. It exits with status `0` when
the worker is ready, `2` when the wait times out and active work still
remains, and `1` on request or connection errors. For multi-worker `drain` and
`resume` calls, the CLI processes workers sequentially over one WebSocket
connection and uses aggregate exit behavior: any request error yields `1`; if
there are no hard errors but at least one `drain` wait times out, the exit code
is `2`; otherwise it is `0`. Pass `--fail-fast` to stop after the first hard
request error. Drain timeouts still contribute to exit code `2`, but they do
not stop the remaining worker list by themselves. Pass `--json` for structured
machine-readable stdout, or `--result-json-out path.json` to also persist the
final structured result with `ok`, `exit_code`, the resolved
`requested_worker_ids`, the actually handled `processed_worker_ids`, and
per-worker outcomes for later deployment-step inspection.
