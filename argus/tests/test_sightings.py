import ipaddress
from datetime import UTC, datetime

from argus_addon.sightings import home_networks, poke, seen_in

# What the Kvarngatan Green answered, trimmed.
ADDRESSES = [
    {"ifname": "lo", "addr_info": [{"family": "inet", "local": "127.0.0.1", "prefixlen": 8, "scope": "host"}]},
    {
        "ifname": "end0",
        "addr_info": [
            {"family": "inet", "local": "192.168.184.2", "prefixlen": 24, "scope": "global"},
            {"family": "inet6", "local": "fe80::ae34", "prefixlen": 64, "scope": "link"},
        ],
    },
    {"ifname": "hassio", "addr_info": [{"family": "inet", "local": "172.30.32.1", "prefixlen": 23, "scope": "global"}]},
    {
        "ifname": "docker0",
        "addr_info": [{"family": "inet", "local": "172.30.232.1", "prefixlen": 23, "scope": "global"}],
    },
    {
        "ifname": "veth52e02d8",
        "addr_info": [{"family": "inet6", "local": "fe80::4055", "prefixlen": 64, "scope": "link"}],
    },
    {
        "ifname": "tailscale0",
        "addr_info": [{"family": "inet", "local": "100.96.7.116", "prefixlen": 32, "scope": "global"}],
    },
]

NEIGHBOURS = [
    {"dst": "172.30.33.0", "dev": "hassio", "lladdr": "ae:89:f5:24:8c:f7", "confirmed": 3, "state": ["REACHABLE"]},
    {"dst": "192.168.184.125", "dev": "end0", "lladdr": "18:e8:29:53:5b:e4", "confirmed": 32, "state": ["REACHABLE"]},
    {"dst": "192.168.184.197", "dev": "end0", "confirmed": 1, "state": ["FAILED"]},
    {"dst": "192.168.184.108", "dev": "end0", "lladdr": "c4:c1:7d:68:ca:1b", "confirmed": 266925, "state": ["STALE"]},
    {"dst": "192.168.184.180", "dev": "end0", "lladdr": "54:2a:1b:57:ab:68", "confirmed": 4, "state": ["DELAY"]},
]


def test_only_the_homes_own_network_is_swept():
    assert home_networks(ADDRESSES) == [("end0", ipaddress.IPv4Interface("192.168.184.2/24"))]


def test_a_device_that_answered_lately_is_seen_when_it_did():
    now = datetime(2026, 10, 20, 21, 0, 0, tzinfo=UTC)
    got = seen_in(NEIGHBOURS, {"end0"}, now)
    assert [(s.mac, s.ip, s.at.strftime("%H:%M:%S")) for s in got] == [
        ("18:E8:29:53:5B:E4", "192.168.184.125", "20:59:28"),
        ("54:2A:1B:57:AB:68", "192.168.184.180", "20:59:56"),
    ], "a failed or long-unconfirmed entry and Home Assistant's own containers are not sightings"


def test_a_subnet_too_large_is_not_swept():
    assert poke(ipaddress.IPv4Interface("10.0.0.2/16")) == 0


async def test_the_loop_sends_each_sweep_and_survives_a_failed_one():
    import asyncio
    import json

    from argus_addon.envelope import Sighting
    from argus_addon.ws_client import _sightings_loop

    sent: list[dict] = []

    class Ws:
        async def send(self, text):
            sent.append(json.loads(text))

    calls = 0

    async def sweeper():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("ip: not found")
        return [Sighting(mac="18:E8:29:53:5B:E4", ip="192.168.184.125", at=datetime(2026, 10, 20, 21, 0, tzinfo=UTC))]

    task = asyncio.create_task(_sightings_loop(Ws(), sweeper=sweeper, every=0.01))
    for _ in range(100):
        if sent:
            break
        await asyncio.sleep(0.01)
    task.cancel()
    assert sent[0]["type"] == "sightings"
    assert sent[0]["devices"][0]["mac"] == "18:E8:29:53:5B:E4"
