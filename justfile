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
        PYTHONUNBUFFERED=1 \
        python -u -m argus_addon

# Internal: runs `just start` and tees its stdout+stderr to argus/tmp/addon.log.
# Single recipe so watchexec doesn't need shell quoting to set up the pipe.
_start-tee:
    @mkdir -p argus/tmp
    just start 2>&1 | tee argus/tmp/addon.log

# Run the addon and restart it on every .py change under argus/.
# Stdout + stderr go to both the terminal AND argus/tmp/addon.log (truncated on each restart).
watch:
    @mkdir -p argus/tmp
    watchexec --restart --exts py --watch argus --ignore 'tmp/**' -- just _start-tee

test:
    cd argus && pytest

fix:
    cd argus && ruff check --fix . && ruff format .

qa:
    cd argus && ruff check . && ruff format --check .

lock:
    cd argus && uv lock

# Re-render argus/icon.png (256x256) and argus/logo.png (500x200) from their SVG sources.
# Pulls librsvg and Inter through nix, so the wordmark renders the same on any machine.
logo:
    #!/usr/bin/env bash
    set -euo pipefail
    inter="$(nix build --no-link --print-out-paths nixpkgs#inter)"
    work="$(mktemp -d)"
    trap 'rm -rf "$work"' EXIT
    cat > "$work/fonts.conf" <<EOF
    <?xml version="1.0"?>
    <!DOCTYPE fontconfig SYSTEM "urn:fontconfig:fonts.dtd">
    <fontconfig>
      <dir>$inter/share/fonts</dir>
      <cachedir>$work/cache</cachedir>
    </fontconfig>
    EOF
    export FONTCONFIG_FILE="$work/fonts.conf"
    nix shell nixpkgs#librsvg --command bash -c '
        rsvg-convert --width=256 --height=256 --output=argus/icon.png argus/icon.svg
        rsvg-convert --width=500 --height=200 --output=argus/logo.png argus/logo.svg
    '
    echo "rendered argus/icon.png and argus/logo.png"

clean-data:
    rm -rf argus/.data
