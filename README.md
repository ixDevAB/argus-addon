# Argus Add-on

A Home Assistant add-on that connects your home to the Argus cloud over a single secure outbound WebSocket. No port forwarding or inbound firewall rules are needed.

## Installation

1. In Home Assistant, go to **Settings → Add-ons → Add-on Store**.
2. Open the menu in the top right (**⋮**) and select **Repositories**.
3. Add this repository URL and click **Add**:

   ```
   https://github.com/ixDevAB/argus-addon
   ```

4. Find **Argus** in the add-on store (refresh the page if it doesn't appear) and click **Install**.
5. Start the add-on. Enable **Start on boot** and **Watchdog** so it stays connected.

Or add the repository with one click:

[![Add repository to my Home Assistant](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/supervisor_add_addon_repository/?repository_url=https%3A%2F%2Fgithub.com%2FixDevAB%2Fargus-addon)

## Pairing with the Argus app

1. After starting the add-on, open the **Argus** panel in the Home Assistant sidebar.
2. The panel shows a claim code.
3. In the Argus app, choose to add a home and enter the claim code.

Once paired, the add-on stores its token and reconnects automatically — no further setup is needed. To unpair, open the Argus panel and use the unpair form.

## Requirements

- Home Assistant OS or Supervised installation (add-ons are not available on Container or Core installs)
- `aarch64` or `amd64` hardware
- Outbound internet access on port 443

## Development

Local development uses Nix + direnv and `just`:

```sh
cp .env.example .env   # set SUPERVISOR_TOKEN, ARGUS_HA_WS_URL, ARGUS_CLOUD_URL
just dev               # run the add-on locally
just test              # run the test suite
```
