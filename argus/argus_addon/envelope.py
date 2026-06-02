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
    domain: Literal["binary_sensor", "switch"]
    friendly_name: str | None = None


class EntityList(BaseModel):
    type: Literal["entity_list"]
    entities: list[EntityRef]


class State(BaseModel):
    type: Literal["state"]
    entity_id: str
    state: str
    at: datetime
    attributes: dict[str, Any] | None = None


class Cmd(BaseModel):
    type: Literal["cmd"]
    id: str
    op: Literal["call_service", "test_trigger"]
    args: dict[str, Any]


class Ack(BaseModel):
    type: Literal["ack"]
    id: str
    duplicate: bool | None = None
    error: str | None = None


Envelope = Annotated[
    Heartbeat | Hello | EntityList | State | Cmd | Ack,
    Field(discriminator="type"),
]

EnvelopeAdapter = TypeAdapter(Envelope)
