"""Which devices answer on the home's network, every minute.

The add-on sends one empty datagram to every address of its own subnet, so the kernel resolves
each by ARP, then reads the neighbour table for the ones that answered and when. A network scanner
probes with raw packets the kernel never sees, so without the sweep the table goes stale.
"""

import asyncio
import ipaddress
import json
import socket
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog

from argus_addon.envelope import Sighting

log = structlog.get_logger(__name__)

SWEEP_EVERY_S = 60.0
# How long the sweep waits for the answers before it reads the table.
ANSWER_WAIT_S = 5.0
# A subnet larger than this is not swept: only what already talks to the add-on is seen.
MAX_HOSTS = 1024
# An entry the kernel confirmed this recently was seen in this sweep.
FRESH_S = 60
# The interfaces Home Assistant and its containers make, never the home's own network.
NOT_HOME_NETWORKS = ("lo", "hassio", "docker", "veth", "tailscale", "br-")
UNANSWERED = {"FAILED", "INCOMPLETE"}


def home_networks(addresses: list[dict[str, Any]]) -> list[tuple[str, ipaddress.IPv4Interface]]:
    """The home's own networks, from `ip -j addr show`: each interface and its address, for the
    private IPv4 subnets the add-on sits on."""
    out: list[tuple[str, ipaddress.IPv4Interface]] = []
    for link in addresses:
        name = str(link.get("ifname", ""))
        if not name or name.startswith(NOT_HOME_NETWORKS):
            continue
        for addr in link.get("addr_info") or []:
            if addr.get("family") != "inet" or addr.get("scope") != "global":
                continue
            iface = ipaddress.IPv4Interface(f"{addr.get('local')}/{addr.get('prefixlen')}")
            if iface.ip.is_private and iface.network.prefixlen < 31:
                out.append((name, iface))
    return out


def poke(iface: ipaddress.IPv4Interface) -> int:
    """Send an empty datagram to every other address of the subnet, so the kernel asks each by
    ARP. Port 9 is discard: nothing answers it, the ARP reply is all that is wanted."""
    if iface.network.num_addresses - 2 > MAX_HOSTS:
        return 0
    sent = 0
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.setblocking(False)
        for host in iface.network.hosts():
            if host == iface.ip:
                continue
            try:
                sock.sendto(b"", (str(host), 9))
                sent += 1
            except OSError:
                continue
    return sent


def seen_in(neighbours: list[dict[str, Any]], devices: set[str], now: datetime) -> list[Sighting]:
    """The devices `ip -s -j neigh` says answered lately, each when it last did."""
    out: list[Sighting] = []
    for entry in neighbours:
        mac = entry.get("lladdr")
        confirmed = entry.get("confirmed")
        state = set(entry.get("state") or [])
        if entry.get("dev") not in devices or not isinstance(mac, str) or not isinstance(confirmed, int):
            continue
        if state & UNANSWERED or confirmed > FRESH_S:
            continue
        out.append(Sighting(mac=mac.upper(), ip=str(entry.get("dst", "")), at=now - timedelta(seconds=confirmed)))
    return out


async def _ip(*args: str) -> Any:
    proc = await asyncio.create_subprocess_exec(
        "ip", *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    out, err = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"ip {' '.join(args)}: {err.decode().strip()}")
    return json.loads(out or b"[]")


async def sweep() -> list[Sighting]:
    """One sweep of the home's networks: every device that answered, and when."""
    networks = home_networks(await _ip("-j", "addr", "show"))
    for _, iface in networks:
        poke(iface)
    await asyncio.sleep(ANSWER_WAIT_S)
    return seen_in(await _ip("-s", "-j", "neigh"), {name for name, _ in networks}, datetime.now(UTC))
