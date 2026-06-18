from eventsourcing.persistence import ApplicationRecorder
from eventsourcing.tests.persistence import NonInterleavingNotificationIDsBaseCase
from kurrentdbclient import KurrentDBClient

from eventsourcing_kurrentdb.recorders import KurrentDBApplicationRecorder
from tests.common import INSECURE_CONNECTION_STRING


class TestNonInterleaving(NonInterleavingNotificationIDsBaseCase):
    insert_num = 1000

    def setUp(self) -> None:
        super().setUp()
        self.client = KurrentDBClient(INSECURE_CONNECTION_STRING)

    def tearDown(self) -> None:
        del self.client

    def create_recorder(self) -> ApplicationRecorder:
        recorder = KurrentDBApplicationRecorder(client=self.client)
        recorder.validate_uuids = True
        return recorder


del NonInterleavingNotificationIDsBaseCase
