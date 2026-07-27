# Changelog

## 0.1.3

- Run under with-contenv so the supervisor token reaches the add-on, fixing Home Assistant auth and sensor sync.

## 0.1.2

- Log the Home Assistant websocket url and the supervisor token length at startup to diagnose auth failures.

## 0.1.1

- Reconnect to Home Assistant after the websocket drops instead of wedging on a closing transport.
- Add a request timeout and websocket heartbeat so a stalled Home Assistant connection recovers.
- Re-subscribe to state changes automatically on every reconnect.
- Forward a top-level entity_id on call_service commands.

## 0.1.0

- First release.
- Pairs with the Argus app from the sidebar panel using a claim code or QR code.
- Connects to the Argus cloud over a single outbound WebSocket on port 443.
- Unpair from the panel to release the home and get a fresh claim code.
