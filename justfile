set shell := ["bash", "-cu"]
set dotenv-load := true

default:
    @just --list

# Run the addon locally against a HA Long-Lived Access Token + Argus cloud URL.
# Required env (put in .env): SUPERVISOR_TOKEN, ARGUS_HA_WS_URL, ARGUS_CLOUD_URL.
dev:
    @test -n "${SUPERVISOR_TOKEN:-}" || (echo "missing SUPERVISOR_TOKEN — set a HA Long-Lived Access Token in .env" && exit 1)
    @test -n "${ARGUS_HA_WS_URL:-}" || (echo "missing ARGUS_HA_WS_URL — e.g. ws://homeassistant.local:8123/api/websocket" && exit 1)
    @test -n "${ARGUS_CLOUD_URL:-}" || (echo "missing ARGUS_CLOUD_URL — e.g. ws://192.168.1.10:8000/ws/addon" && exit 1)
    mkdir -p argus/.data
    cd argus && \
        ARGUS_TOKEN_PATH="${ARGUS_TOKEN_PATH:-$(pwd)/.data/token.txt}" \
        ARGUS_IDEMPOTENCY_PATH="${ARGUS_IDEMPOTENCY_PATH:-$(pwd)/.data/idempotency.db}" \
        python -m argus_addon

test:
    cd argus && pytest

fix:
    cd argus && ruff check --fix . && ruff format .

qa:
    cd argus && ruff check . && ruff format --check .

lock:
    cd argus && uv lock

clean-data:
    rm -rf argus/.data
