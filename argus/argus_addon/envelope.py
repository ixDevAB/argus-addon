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
    domain: Literal["binary_sensor", "switch", "siren", "light", "number", "select", "camera", "sensor"]
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


class StateSnapshotItem(BaseModel):
    entity_id: str
    state: str
    attributes: dict[str, Any] | None = None


class StatesSnapshot(BaseModel):
    type: Literal["states"]
    states: list[StateSnapshotItem]


class Cmd(BaseModel):
    type: Literal["cmd"]
    id: str
    op: Literal["call_service", "test_trigger", "resync", "webrtc_offer", "webrtc_close", "talk_offer", "talk_close"]
    args: dict[str, Any]


class Ack(BaseModel):
    type: Literal["ack"]
    id: str
    duplicate: bool | None = None
    error: str | None = None
    # Reply payload for commands that answer with data (webrtc_offer -> session_id + sdp).
    result: dict[str, Any] | None = None


Envelope = Annotated[
    Heartbeat | Hello | EntityList | State | Cmd | Ack,
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
