from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, TypeAdapter


class Heartbeat(BaseModel):
    type: Literal["hb"]
    ts: datetime


class Hello(BaseModel):
    type: Literal["hello"]
    addon_version: str
    ha_version: str


class EntityRef(BaseModel):
    entity_id: str
    device_class: str | None
    domain: Literal[
        "binary_sensor", "switch", "siren", "light", "number", "select", "camera", "sensor", "button", "lock"
    ]
    friendly_name: str | None = None
    area: str | None = None
    entity_category: str | None = None
    # Device grouping (so Argus can present a camera's entities together) and, for
    # writable config entities (number/select), the current value + bounds/options.
    device_id: str | None = None
    device_name: str | None = None
    value: str | None = None
    attributes: dict[str, Any] | None = None
    # Which integration owns the entity and HA's language-neutral key for what it measures.
    # Camera AI classes (Reolink person/vehicle/animal) share device_class `motion` and have
    # localized entity_ids, so the cloud tells them apart by translation_key.
    platform: str | None = None
    translation_key: str | None = None
    # The owning integration's iot_class from its manifest (cloud_push, local_polling, ...). A
    # cloud device can be reported by more than one home, a local one is in the home that sees it.
    iot_class: str | None = None


class DeviceRef(BaseModel):
    """A physical unit from HA's device registry: what the Units tab shows."""

    id: str
    name: str | None = None
    manufacturer: str | None = None
    model: str | None = None
    sw_version: str | None = None
    hw_version: str | None = None
    area: str | None = None
    via_device_id: str | None = None


class EntityList(BaseModel):
    type: Literal["entity_list"]
    entities: list[EntityRef]
    devices: list[DeviceRef] = []


class State(BaseModel):
    type: Literal["state"]
    entity_id: str
    state: str
    at: datetime
    attributes: dict[str, Any] | None = None
    # How long ago the change happened, by this box's clock, when the frame went out: a change held
    # while the cloud link was down arrives old, and the cloud must not judge it as happening now.
    age_ms: int | None = None


class StateSnapshotItem(BaseModel):
    entity_id: str
    state: str
    attributes: dict[str, Any] | None = None


class StatesSnapshot(BaseModel):
    type: Literal["states"]
    states: list[StateSnapshotItem]


class Tracker(BaseModel):
    """A person or a device tracker in Home Assistant: who is home, and which phones the router sees."""

    entity_id: str
    kind: Literal["person", "device_tracker"]
    name: str | None = None
    state: str
    source_type: str | None = None
    device_trackers: list[str] | None = None
    # The network address a scanner or router reports, upper case; None for a phone's app tracker.
    mac: str | None = None
    changed_at: datetime
    # When a network scanner last reached the device; its "gone" comes only after consider_home,
    # so this is when it really left. None when the tracker does not say.
    last_seen_at: datetime | None = None


class Presence(BaseModel):
    """Every tracker when full, else the ones whose state just changed.

    A replay carries replay_from: trackers are then every change of the listed trackers since that
    moment, oldest first, from Home Assistant's own history, and the cloud takes them as the record
    of that period, so a change it missed or dated late is put right.
    """

    type: Literal["presence"]
    full: bool
    trackers: list[Tracker]
    replay_from: datetime | None = None


class Sighting(BaseModel):
    """A device that answered on the home's network, and when it last did; since, when it carries a
    whole stretch the scanner reached it, from the first scan of it."""

    mac: str
    ip: str
    at: datetime
    since: datetime | None = None


class Sightings(BaseModel):
    """Every device that answered one sweep of the home's network."""

    type: Literal["sightings"]
    devices: list[Sighting]


class Cmd(BaseModel):
    type: Literal["cmd"]
    id: str
    op: Literal[
        "call_service",
        "test_trigger",
        "resync",
        "webrtc_offer",
        "webrtc_close",
        "talk_offer",
        "talk_close",
        "record_clip",
        "extend_clip",
        "upload_clip",
        "cancel_clip",
        "unpair",
        "rotate_token",
    ]
    args: dict[str, Any]


class Clip(BaseModel):
    """Progress of a recording: the cloud checks the keys against the ones it chose."""

    type: Literal["clip"]
    clip_id: str
    status: Literal[
        "recording", "poster_uploaded", "init_uploaded", "segment_uploaded", "uploaded", "spooled", "failed"
    ]
    media_started_at: str | None = None
    poster_key: str | None = None
    object_key: str | None = None
    index: int | None = None
    segments: int | None = None
    bytes: int | None = None
    duration_ms: int | None = None
    codec: str | None = None
    width: int | None = None
    height: int | None = None
    reason: str | None = None
    detail: str | None = None


class Ack(BaseModel):
    type: Literal["ack"]
    id: str
    duplicate: bool | None = None
    error: str | None = None
    # Reply payload for commands that answer with data (webrtc_offer -> session_id + sdp).
    result: dict[str, Any] | None = None


Envelope = Annotated[
    Heartbeat | Hello | EntityList | State | Cmd | Ack | Clip,
    Field(discriminator="type"),
]

EnvelopeAdapter = TypeAdapter(Envelope)


class PairCode(BaseModel):
    type: Literal["pair_code"]
    session_id: str
    code: str
    expires_at: datetime


class PairToken(BaseModel):
    type: Literal["pair_token"]
    install_token: str


PairEnvelope = Annotated[
    PairCode | PairToken,
    Field(discriminator="type"),
]

PairEnvelopeAdapter = TypeAdapter(PairEnvelope)
