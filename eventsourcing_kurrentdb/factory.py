from __future__ import annotations

from typing import TYPE_CHECKING, Literal, cast

from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    InfrastructureFactory,
    InfrastructureFactoryError,
    ProcessRecorder,
    TrackingRecorder,
)
from kurrentdbclient import KurrentDBClient

from eventsourcing_kurrentdb.recorders import (
    KurrentDBAggregateRecorder,
    KurrentDBApplicationRecorder,
)

if TYPE_CHECKING:
    from eventsourcing.utils import Environment


class KurrentDBFactory(InfrastructureFactory[TrackingRecorder]):
    """
    Infrastructure factory for KurrentDB infrastructure.
    """

    KURRENTDB_URI = "KURRENTDB_URI"
    KURRENTDB_ROOT_CERTIFICATES = "KURRENTDB_ROOT_CERTIFICATES"
    ORIGINATOR_ID_TYPE = "ORIGINATOR_ID_TYPE"

    def __init__(self, env: Environment):
        super().__init__(env)
        eventstoredb_uri = self.env.get(self.KURRENTDB_URI)
        if eventstoredb_uri is None:
            msg = (
                f"{self.KURRENTDB_URI!r} not found "
                "in environment with keys: "
                f"{', '.join(self.env.create_keys(self.KURRENTDB_URI))!r}"
            )
            raise InfrastructureFactoryError(msg)
        root_certificates = self.env.get(self.KURRENTDB_ROOT_CERTIFICATES)
        self.client = KurrentDBClient(
            uri=eventstoredb_uri,
            root_certificates=root_certificates,
        )
        originator_id_type = cast(
            Literal["uuid", "text"],
            self.env.get(self.ORIGINATOR_ID_TYPE, "uuid"),
        )
        if originator_id_type.lower() not in ("uuid", "text"):
            msg = (
                f"Invalid {self.ORIGINATOR_ID_TYPE} '{originator_id_type}', "
                f"must be 'uuid' or 'text'"
            )
            raise InfrastructureFactoryError(msg)
        self.originator_id_type = originator_id_type

    def aggregate_recorder(self, purpose: str = "events") -> AggregateRecorder:
        recorder = KurrentDBAggregateRecorder(
            client=self.client,
            for_snapshotting=bool(purpose == "snapshots"),
        )
        recorder.validate_uuids = self.originator_id_type == "uuid"
        return recorder

    def application_recorder(self) -> ApplicationRecorder:
        recorder = KurrentDBApplicationRecorder(self.client)
        recorder.validate_uuids = self.originator_id_type == "uuid"
        return recorder

    def tracking_recorder(
        self, tracking_recorder_class: type[TrackingRecorder] | None = None
    ) -> TrackingRecorder:
        raise NotImplementedError

    def process_recorder(self) -> ProcessRecorder:
        raise NotImplementedError

    def __del__(self) -> None:
        if hasattr(self, "client"):
            del self.client
            # self.client.close()
