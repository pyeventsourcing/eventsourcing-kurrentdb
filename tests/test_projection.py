from __future__ import annotations

from abc import abstractmethod
from typing import Any, TypedDict
from unittest import TestCase
from uuid import NAMESPACE_URL, uuid4, uuid5

from eventsourcing.domain import event
from eventsourcing.persistence import Tracking, TrackingRecorder
from eventsourcing.popo import POPOTrackingRecorder
from eventsourcing.projection import Projection, ProjectionRunner
from eventsourcing.pydantic import Aggregate, AggregatesApplication, Decision
from eventsourcing.utils import get_topic

from tests.common import INSECURE_CONNECTION_STRING


class TrainingSchool(AggregatesApplication):
    def register(self, name: str) -> int:
        dog = Dog(name=name)
        recordings = self.save(dog)
        return recordings[-1].notification.id

    def add_trick(self, name: str, trick: str) -> int:
        dog = self._get_dog(name)
        dog.add_trick(trick)
        recordings = self.save(dog)
        return recordings[-1].notification.id

    def get_dog_details(self, name: str) -> DogDetails:
        dog = self._get_dog(name)
        return {"name": dog.name, "tricks": tuple(dog.tricks)}

    def _get_dog(self, name: str) -> Dog:
        return self.repository.get(Dog.create_id(name), Dog)


class Dog(Aggregate):
    INITIAL_VERSION = 0  # for KurrentDB

    class Registered(Decision):
        name: str

    class TrickAdded(Decision):
        trick: str

    @staticmethod
    def create_id(name: str) -> str:
        return str(uuid5(NAMESPACE_URL, f"/dogs/{name}"))

    @event(Registered)
    def __init__(self, name: str) -> None:
        self.name = name
        self.tricks: list[str] = []

    @event(TrickAdded)
    def add_trick(self, trick: str) -> None:
        self.tricks.append(trick)


class DogDetails(TypedDict):
    name: str
    tricks: tuple[str, ...]


class CounterViewInterface(TrackingRecorder):
    @abstractmethod
    def get_dog_counter(self) -> int:
        pass

    @abstractmethod
    def get_trick_counter(self) -> int:
        pass

    @abstractmethod
    def incr_dog_counter(self, tracking: Tracking) -> None:
        pass

    @abstractmethod
    def incr_trick_counter(self, tracking: Tracking) -> None:
        pass


class POPOCountRecorder(POPOTrackingRecorder, CounterViewInterface):
    def __init__(self) -> None:
        super().__init__()
        self._dog_counter = 0
        self._trick_counter = 0

    def get_dog_counter(self) -> int:
        return self._dog_counter

    def get_trick_counter(self) -> int:
        return self._trick_counter

    def incr_dog_counter(self, tracking: Tracking) -> None:
        with self._database_lock:
            self._insert_tracking(tracking)
            self._dog_counter += 1

    def incr_trick_counter(self, tracking: Tracking) -> None:
        with self._database_lock:
            self._insert_tracking(tracking)
            self._trick_counter += 1


class CountProjection(Projection[CounterViewInterface]):
    topics = (
        get_topic(Dog.Registered),
        get_topic(Dog.TrickAdded),
    )

    def __init__(self, view: CounterViewInterface):
        super().__init__(view=view)

    def process_event(self, envelope: Any, tracking: Tracking) -> None:
        match envelope.decision:
            case Dog.Registered():
                self.view.incr_dog_counter(tracking)
            case Dog.TrickAdded():
                self.view.incr_trick_counter(tracking)


class TestProjection(TestCase):
    def test_projection(self) -> None:
        env = {
            "TRAININGSCHOOL_PERSISTENCE_MODULE": "eventsourcing_kurrentdb",
            "TRAININGSCHOOL_KURRENTDB_URI": INSECURE_CONNECTION_STRING,
        }
        training_school = TrainingSchool(env=env)

        dog_name = f"Fido-{uuid4()}"
        training_school.register(dog_name)
        training_school.add_trick(dog_name, "roll over")
        training_school.add_trick(dog_name, "play dead")
        dog_details = training_school.get_dog_details(dog_name)
        assert dog_details["name"] == dog_name
        assert dog_details["tricks"] == ("roll over", "play dead")

        dog_details = training_school.get_dog_details(dog_name)

        assert dog_details["name"] == dog_name
        assert dog_details["tricks"] == ("roll over", "play dead")

        with ProjectionRunner(
            application_class=TrainingSchool,
            projection_class=CountProjection,
            view_class=POPOCountRecorder,
            env=env,
        ) as runner:
            # Get "read model" instance from runner, because
            # state of materialised view is stored in memory.
            materialised_view = runner.projection.view

            # Wait for the existing events to be processed.
            materialised_view.wait(
                application_name=training_school.name,
                notification_id=training_school.recorder.max_notification_id(),
                timeout=100,
                interrupt=runner.is_interrupted,
            )

            # Query the "read model".
            dog_count = materialised_view.get_dog_counter()
            trick_count = materialised_view.get_trick_counter()

            # Record another event in "write model".
            notification_id = training_school.add_trick(dog_name, "sit and stay")

            # Wait for the new event to be processed by the projection.
            materialised_view.wait(
                application_name=training_school.name,
                notification_id=notification_id,
                timeout=1,
                interrupt=runner.is_interrupted,
            )

            # Expect one trick more, same number of dogs.
            assert dog_count == materialised_view.get_dog_counter()
            assert trick_count + 1 == materialised_view.get_trick_counter()
            # print("Trick count:", trick_count)

            # Write another event.
            notification_id = training_school.add_trick(dog_name, "jump hoop")

            # Wait for the new event to be processed.
            materialised_view.wait(
                training_school.name,
                notification_id,
            )

            # Expect two tricks more, same number of dogs.
            assert dog_count == materialised_view.get_dog_counter()
            assert trick_count + 2 == materialised_view.get_trick_counter()
