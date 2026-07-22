# from __future__ import annotations
#
# from dataclasses import dataclass, field
# from typing import TYPE_CHECKING, Any
# from uuid import NAMESPACE_URL, UUID, uuid5
#
# from eventsourcing.domain import (
#     datetime_now_with_tzinfo,
#     event,
#     get_metadata_from_context,
# )
#
# if TYPE_CHECKING:
#     from datetime import datetime
#
#
# @dataclass(frozen=True, kw_only=True)
# class DomainEvent(metaclass=MetaDomainEvent):
#     originator_id: str
#     originator_version: int
#     timestamp: datetime = field(default_factory=datetime_now_with_tzinfo)
#     metadata: dict[str, str] = field(default_factory=get_metadata_from_context)
#     event_id: UUID = NIL_UUID
#
#     def __post_init__(self) -> None:
#         if not isinstance(self.originator_id, str):
#             msg = (
#                 f"{type(self)} "
#                 f"was initialized with a non-str originator_id: "
#                 f"{self.originator_id!r}"
#             )
#             raise TypeError(msg)
#         if self.event_id == NIL_UUID:
#             deterministic_id = event_id_from_originator_id_and_version(
#                 self.originator_id,
#                 self.originator_version,
#             )
#             object.__setattr__(self, "event_id", deterministic_id)
#
#
# @dataclass(frozen=True)
# class Snapshot(DomainEvent, CanSnapshotAggregate[str]):
#     topic: str
#     state: dict[str, Any]
#
#
# class Aggregate(BaseAggregate[str]):
#     @dataclass(frozen=True)
#     class Event(DomainEvent, CanMutateAggregate[str]):
#         pass
#
#     @dataclass(frozen=True)
#     class Created(Event, CanInitAggregate[str]):
#         originator_topic: str
#
#
# class Dog(Aggregate):
#     INITIAL_VERSION = 0
#
#     @staticmethod
#     def create_id(name: str) -> str:
#         return "dog-" + str(uuid5(NAMESPACE_URL, f"/dogs/{name}"))
#
#     @dataclass(frozen=True)
#     class Registered(Aggregate.Created):
#         name: str
#
#     @dataclass(frozen=True)
#     class TrickAdded(Aggregate.Event):
#         trick: str
#
#     @event(Registered)
#     def __init__(self, name: str) -> None:
#         self.name = name
#         self.tricks: list[str] = []
#
#     @event(TrickAdded)
#     def add_trick(self, trick: str) -> None:
#         self.tricks.append(trick)
