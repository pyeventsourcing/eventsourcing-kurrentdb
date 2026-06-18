from __future__ import annotations

import contextlib
import sys
import uuid
from unittest import TestCase, skipIf

from tests.stringids.application import DogSchool
from tests.stringids.domainmodel import Dog

with contextlib.suppress(ImportError):
    import eventsourcing_kurrentdb  # noqa: F401  # pyright: ignore[reportMissingImports]


class TestDogSchool(TestCase):
    def setUp(self) -> None:
        self.env: dict[str, str] = {}

    def test_dog_school(self) -> None:
        # Construct application object.
        school = DogSchool(self.env)

        max_notification_id = school.recorder.max_notification_id()

        # Evolve application state.
        dog_name = f"Fido-{uuid.uuid4()}"
        school.register_dog(dog_name)
        school.add_trick(dog_name, "roll over")
        school.add_trick(dog_name, "play dead")

        # Query application state.
        dog = school.get_dog(dog_name)
        self.assertEqual(dog_name, dog["name"])
        self.assertEqual(("roll over", "play dead"), dog["tricks"])

        # Select notifications.
        notifications = school.notification_log.select(
            start=max_notification_id, limit=10, inclusive_of_start=False
        )
        self.assertEqual(3, len(notifications))

        # Take snapshot.
        school.take_snapshot(Dog.create_id(dog_name), version=3)
        dog = school.get_dog(dog_name)
        self.assertEqual(dog_name, dog["name"])
        self.assertEqual(("roll over", "play dead"), dog["tricks"])

        # Continue with snapshotted aggregate.
        school.add_trick(dog_name, "fetch ball")
        dog = school.get_dog(dog_name)
        self.assertEqual(dog_name, dog["name"])
        self.assertEqual(("roll over", "play dead", "fetch ball"), dog["tricks"])

    def test_dog_school_with_sqlite(self) -> None:
        self.env["PERSISTENCE_MODULE"] = "eventsourcing.sqlite"
        self.env["SQLITE_DBNAME"] = ":memory:"
        self.test_dog_school()

    @skipIf("eventsourcing_kurrentdb" not in sys.modules, "KurrentDB not installed")
    def test_dog_school_with_kurrentdb(self) -> None:
        self.env["PERSISTENCE_MODULE"] = "eventsourcing_kurrentdb"
        self.env["KURRENTDB_URI"] = "esdb://localhost:2113?Tls=false"
        self.test_dog_school()
