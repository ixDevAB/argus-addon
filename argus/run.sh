#!/usr/bin/with-contenv bashio
bashio::log.info "Starting Argus"
exec python -m argus_addon
