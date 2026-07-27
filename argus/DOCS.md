# Argus

Connects this Home Assistant to the Argus cloud over a single outbound WebSocket on port 443. Nothing listens on your network, so no port forwarding or inbound firewall rule is needed.

## Pair with the Argus app

1. Start the add-on, then turn on **Start on boot** and **Watchdog** so it comes back after a reboot.
2. Open **Argus** in the Home Assistant sidebar.
3. The panel shows a claim code and a QR code.
4. In the Argus app, add a home, then scan the QR code or type the claim code.

The panel switches to "Argus is paired" once the app confirms. After that the add-on reconnects on its own — there is nothing else to set up.

## Unpair

Open the **Argus** panel and use the **Unpair** button. The add-on drops its token, returns to pairing mode, and shows a fresh claim code — so you can hand the home over to a different Argus account.

## Configuration

None. The add-on has no options; everything it needs arrives through pairing.

## What it stores

Both files live in the add-on's own data volume and survive restarts and updates:

| File                   | Purpose                                                                        |
| ---------------------- | ------------------------------------------------------------------------------ |
| `/data/token.txt`      | The cloud token issued at pairing. Removed when you unpair.                    |
| `/data/idempotency.db` | Tracks handled commands so a reconnect cannot run the same one twice.          |

A full Home Assistant backup includes both, so restoring a backup also restores the pairing.

## Requirements

- Home Assistant OS or Supervised — add-ons are not available on Container or Core installs
- `aarch64` or `amd64` hardware
- Outbound HTTPS on port 443

## Troubleshooting

Start with the add-on **Log** tab; every failure path logs there.

| Symptom                          | What to check                                                                                          |
| -------------------------------- | ------------------------------------------------------------------------------------------------------ |
| No **Argus** entry in the sidebar | The add-on has to be running for its panel to appear. Start it, then reload the page.                  |
| The claim code stopped working    | Claim codes carry an expiry. Reload the panel to get a fresh one.                                      |
| The log repeats connection retries | Outbound 443 to the Argus cloud is being blocked. Check the router or firewall, then restart the add-on. |
| Paired, but the app shows nothing | The add-on reads entity states through the Home Assistant API. Restart it and check the log for API errors. |
