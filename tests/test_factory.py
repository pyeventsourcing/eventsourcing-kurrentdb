import os
from unittest import TestCase

from eventsourcing.errors import InfrastructureFactoryError
from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    InfrastructureFactory,
    ProcessRecorder,
    TrackingRecorder,
)
from eventsourcing.tests.persistence import InfrastructureFactoryTestCase
from eventsourcing.utils import Environment

from eventsourcing_kurrentdb.factory import KurrentDBFactory
from eventsourcing_kurrentdb.recorders import (
    KurrentDBAggregateRecorder,
    KurrentDBApplicationRecorder,
)
from tests.common import INSECURE_CONNECTION_STRING


class TestFactory(InfrastructureFactoryTestCase[KurrentDBFactory]):
    class SubApplicationRecorder(KurrentDBApplicationRecorder):
        pass

    def setUp(self) -> None:
        self.env = Environment("TestCase")
        self.env[InfrastructureFactory.PERSISTENCE_MODULE] = KurrentDBFactory.__module__
        self.env[KurrentDBFactory.KURRENTDB_URI] = INSECURE_CONNECTION_STRING
        super().setUp()

    def tearDown(self) -> None:
        if KurrentDBFactory.KURRENTDB_URI in os.environ:
            del os.environ[KurrentDBFactory.KURRENTDB_URI]
        super().tearDown()

    def test_create_process_recorder(self) -> None:
        self.skipTest("KurrentDB doesn't support tracking records")

    def expected_factory_class(self) -> type[KurrentDBFactory]:
        return KurrentDBFactory

    def expected_aggregate_recorder_class(self) -> type[AggregateRecorder]:
        return KurrentDBAggregateRecorder

    def expected_application_recorder_class(self) -> type[ApplicationRecorder]:
        return KurrentDBApplicationRecorder

    def expected_tracking_recorder_class(self) -> type[TrackingRecorder]:
        raise NotImplementedError

    def tracking_recorder_subclass(self) -> type[TrackingRecorder]:
        raise NotImplementedError

    def application_recorder_subclass(self) -> type[ApplicationRecorder]:
        return self.SubApplicationRecorder

    def process_recorder_subclass(self) -> type[ProcessRecorder]:
        raise NotImplementedError

    def test_create_tracking_recorder(self) -> None:
        self.skipTest("KurrentDB doesn't support tracking records")

    def expected_process_recorder_class(self) -> type[ProcessRecorder]:
        raise NotImplementedError


class TestFactoryEnvironmentError(TestCase):
    def test_originator_id_type_invalid(self) -> None:
        with self.assertRaises(InfrastructureFactoryError) as cm:
            KurrentDBFactory(
                Environment(
                    env={
                        KurrentDBFactory.KURRENTDB_URI: INSECURE_CONNECTION_STRING,
                        KurrentDBFactory.ORIGINATOR_ID_TYPE: "int",
                    }
                )
            )
        self.assertEqual(
            "Invalid ORIGINATOR_ID_TYPE 'int', must be 'uuid' or 'text'",
            str(cm.exception),
        )


del InfrastructureFactoryTestCase
