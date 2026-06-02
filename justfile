set shell := ["bash", "-cu"]
set dotenv-load := true
set dotenv-filename := ".env.local"

default:
    @just --list

# Spot-check that .env.local is loaded. Prints public URLs and only the length of SUPERVISOR_TOKEN (never the token itself).
env:
    @echo "ARGUS_HA_WS_URL = ${ARGUS_HA_WS_URL:-<unset>}"
    @echo "ARGUS_CLOUD_URL = ${ARGUS_CLOUD_URL:-<unset>}"
    @echo "SUPERVISOR_TOKEN length = ${#SUPERVISOR_TOKEN}"

# Run the addon locally. Required env (.env.local): SUPERVISOR_TOKEN, ARGUS_HA_WS_URL, ARGUS_CLOUD_URL.
start:
    @test -n "${SUPERVISOR_TOKEN:-}" || (echo "missing SUPERVISOR_TOKEN — paste your HA Long-Lived Access Token into .env.local" && exit 1)
    @test -n "${ARGUS_HA_WS_URL:-}" || (echo "missing ARGUS_HA_WS_URL — e.g. ws://homeassistant.local:8123/api/websocket" && exit 1)
    @test -n "${ARGUS_CLOUD_URL:-}" || (echo "missing ARGUS_CLOUD_URL — e.g. ws://127.0.0.1:8000/ws/addon" && exit 1)
    mkdir -p argus/.data
    cd argus && \
        ARGUS_TOKEN_PATH="${ARGUS_TOKEN_PATH:-$(pwd)/.data/token.txt}" \
        ARGUS_IDEMPOTENCY_PATH="${ARGUS_IDEMPOTENCY_PATH:-$(pwd)/.data/idempotency.db}" \
        python -m argus_addon

# Run the addon and restart it on every .py change under argus/.
# Stdout + stderr are tee'd to argus/tmp/addon.log (truncated on each restart).
watch:
    mkdir -p argus/tmp
    watchexec --restart --exts py --watch argus --ignore 'tmp/**' -- bash -c 'just start 2>&1 | tee argus/tmp/addon.log'

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
