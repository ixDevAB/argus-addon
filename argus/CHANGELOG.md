# Changelog

## 0.1.18

- Forward number and select state changes (PIR sensitivity, AI sensitivities, day/night mode) so a setting changed in the app or in the Reolink app shows its real value in Argus.

## 0.1.17

- Resend the state snapshot whenever the link to Home Assistant Core comes back. A Core restart marks entities unavailable and restores them while the add-on is reconnecting, and those transitions never arrived as events, so a sensor that had not moved since stayed silent in Argus until it did.

## 0.1.16

- Send the devices behind the synced entities (name, manufacturer, model, firmware, hardware, area, hub) so Argus can show each physical unit, what it provides, and its firmware.

## 0.1.15

- Push-to-talk through the add-on's own go2rtc. Home Assistant opens the camera without its return channel, so talk audio sent that way is dropped; the add-on now opens the hub's RTSP with the backchannel for the duration of a talk session. Enter the Reolink hub's address and login in the add-on options; they stay on this box. The add-on uses host networking so the talk session can reach it.

## 0.1.14

- Re-establish the Home Assistant link and the state subscription on its own after it drops (a Core restart). Until now the add-on stayed deaf to sensor changes until the cloud happened to relay a command.

## 0.1.13

- Sync each camera's charging state (Reolink's battery state enum: discharging / charging / charge complete). The entity ships disabled in Home Assistant, so the add-on enables it in the entity registry and resyncs once Home Assistant has created it.

## 0.1.12

- Include each entity's `platform` and `translation_key` from the entity registry, so Argus can tell a camera's person / vehicle / animal detections apart from its generic motion sensor (they share device class `motion` and have localized entity ids).

## 0.1.11

- Sync battery-level sensors (`sensor.*` with device class `battery`, e.g. Reolink cameras' own battery entity) and forward their changes, so Argus can warn when a camera needs charging.

## 0.1.10

- Skip entities that are disabled in Home Assistant's entity registry. They have no state and refuse streams and service calls, so Argus must never offer them (Reolink's disabled stream profiles showed up as cameras that could not play).

## 0.1.9

- Forward HA `camera.*` entities (and their availability) so cameras can be mapped in Argus.
- Handle `webrtc_offer` / `webrtc_close` commands: negotiate a WebRTC session with a camera through Home Assistant's own WebRTC API and fold the trickled ICE candidates into the answer, so the Argus app can stream live video without exposing Home Assistant.

## 0.1.6

- Forward HA `siren.*` entities (e.g. camera sirens) so they can be mapped as sirens in Argus.
- Include each entity's `entity_category` so Argus can auto-hide config/diagnostic clutter.

## 0.1.5

- Send a current-state snapshot on connect and resync so Argus knows which doors and windows are open right away, not only after the next change.

## 0.1.4

- Handle a resync command from the Argus cloud by re-fetching Home Assistant entities and re-sending the entity list, so renames and device-class changes made in Home Assistant can be pulled into Argus on demand.

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
