# Argus Add-on

A Home Assistant add-on that bridges a local HA instance to the Argus cloud over a single secure **outbound** WebSocket. It relays commands from the cloud to HA services, reports state, and pairs the Home to a cloud account via an ingress UI.

The Python package lives under `argus/`; the repo root holds dev tooling and the add-on store metadata.

## Architecture

Two WebSocket connections run concurrently, started from `argus/argus_addon/__main__.py`:

1. **Cloud link** (`ws_client.py`) — outbound to `wss://ws.argus.ixdev.se/ws/addon/{token}`. Waits for the pairing token file to exist, then connects with exponential backoff + jitter, runs a heartbeat task and a send-queue drain task, and handles inbound `Cmd` envelopes.
2. **HA link** (`ha_client.py`) — connects to the HA Core WS API (`ws://supervisor/core/websocket`), authenticates with `SUPERVISOR_TOKEN`, and exposes `fetch_entities` (filtered to `binary_sensor`/`switch`), `subscribe_events`, and `call_service`. Uses an id→Future request/reply pattern with a background reader.

Supporting modules:

- `envelope.py` — the wire protocol: a pydantic discriminated union (`hb`, `hello`, `entity_list`, `state`, `cmd`, `ack`) keyed on `type`, validated via `EnvelopeAdapter`. The pairing frames (`pair_code`, `pair_token`) live in a separate `PairEnvelope` union (`PairEnvelopeAdapter`) so they never touch the alarm-critical path.
- `idempotency.py` — SQLite dedup of command ids. Replayed `Cmd`s are ack'd with `duplicate=True` and never re-executed.
- `heartbeat.py` — emits `hb` frames on an interval (15s).
- `ingress.py` — aiohttp app on port 8099. When unpaired it displays the current claim-code (the latest `pair_code` from `pair_client`) for the user to enter in the Argus app; when paired it shows an unpair form. The user never types or scans anything into the add-on.
- `pair_client.py` — first-run claim-code pairing. With no `/data` token it connects the token-less provisional `/ws/pair` path (derived from `cloud_url`) with the same full-jitter backoff, holds the latest `pair_code` in memory (`CodeHolder`, never persisted) for ingress + a log line, and on `pair_token` writes the install token to `/data/token.txt` (`0600`, never logged), then hands off to `ws_client` for the authenticated `/ws/addon/<token>` connection.

**Command flow:** cloud sends `Cmd(op=call_service)` → dedup check → relayed to HA via `call_service` → `Ack` (with `error` on failure). Only `call_service` is dispatched today (`test_trigger` is defined in the protocol but not handled).

## Commands

Run via `just` (see `justfile`); all wrap actions inside `argus/`.

- `just dev` — run the add-on locally. Requires `SUPERVISOR_TOKEN`, `ARGUS_HA_WS_URL`, `ARGUS_CLOUD_URL` in `.env` (copy `.env.example`). Writes token/idempotency state under `argus/.data/`.
- `just test` — `pytest` (asyncio auto-mode, 10s per-test timeout).
- `just fix` — `ruff check --fix` + `ruff format`.
- `just qa` — `ruff check` + `ruff format --check` (no writes).
- `just lock` — `uv lock`.
- `just clean-data` — remove `argus/.data/`.

Run a single test: `cd argus && pytest tests/test_ws_client.py -k handshake`.

## Dev environment

- Nix flake (`flake.nix`) provides the devShell via uv2nix (Python 3.12, `just`, `uv`); `.envrc` loads it through direnv plus `.env`.
- Dependencies are declared in `argus/pyproject.toml` and pinned in `argus/uv.lock`.

## Runtime / deployment

- `argus/config.yaml` — HA add-on manifest (ingress on 8099, `data:rw` map, arch `aarch64`/`amd64`).
- `argus/Dockerfile` — builds on the HA base-python image, `pip install .`, runs `run.sh`.
- `argus/run.sh` — bashio entrypoint, `exec python -m argus_addon`.
- `repository.yaml` — add-on store metadata.
- In production, paths default to `/data/token.txt` and `/data/idempotency.db`; all paths and URLs are overridable via env vars (`ARGUS_TOKEN_PATH`, `ARGUS_IDEMPOTENCY_PATH`, `ARGUS_CLOUD_URL`, `ARGUS_HA_WS_URL`).

## Testing

Tests live in `argus/tests/`. `_mocks/cloud_mock.py` and `_mocks/ha_mock.py` stand in for the cloud and HA Core WS servers (exposed as the `cloud_mock` / `ha_mock` fixtures in `tests/conftest.py`). `test_dry_run_pipeline.py` and `test_ws_client.py` exercise the end-to-end relay and idempotent replay against `cloud_mock`.

## Conventions

- Ruff: line-length 120, `E501` ignored, double-quote style. Static assets excluded from lint.
- Async throughout (asyncio + aiohttp + websockets); structlog for logging.
