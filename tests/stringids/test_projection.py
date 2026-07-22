# from typing import Any
# from unittest import TestCase
# from uuid import uuid4
#
# from eventsourcing.dispatch import singledispatchmethod
# from eventsourcing.persistence import Tracking
# from eventsourcing.projection import Projection, ProjectionRunner
# from eventsourcing.utils import get_topic
#
# from tests.common import INSECURE_CONNECTION_STRING
# from tests.stringids.application import DogSchool
# from tests.stringids.domainmodel import Dog
# from tests.test_projection import CounterViewInterface, POPOCountRecorder
#
#
# class CountProjection(Projection[CounterViewInterface]):
#     topics = (
#         get_topic(Dog.Registered),
#         get_topic(Dog.TrickAdded),
#     )
#
#     def __init__(self, view: CounterViewInterface):
#         super().__init__(view=view)
#
#     @singledispatchmethod
#     def process_event(self, event: Any, tracking: Tracking) -> None:
#         print("Ignoring event:", event)
#
#     @process_event.register
#     def aggregate_created(self, _: Dog.Registered, tracking: Tracking) -> None:
#         self.view.incr_dog_counter(tracking)
#
#     @process_event.register
#     def aggregate_event(self, _: Dog.TrickAdded, tracking: Tracking) -> None:
#         self.view.incr_trick_counter(tracking)
#
#
# class TestProjection(TestCase):
#     def test_projection(self) -> None:
#         env = {
#             "DOGSCHOOL_PERSISTENCE_MODULE": "eventsourcing_kurrentdb",
#             "DOGSCHOOL_KURRENTDB_URI": INSECURE_CONNECTION_STRING,
#         }
#         training_school = DogSchool(env=env)
#
#         dog_name = f"Fido-{uuid4()}"
#         training_school.register_dog(dog_name)
#         training_school.add_trick(dog_name, "roll over")
#         notification_id = training_school.add_trick(dog_name, "play dead")
#         dog_details = training_school.get_dog(dog_name)
#         assert dog_details["name"] == dog_name
#         assert dog_details["tricks"] == ("roll over", "play dead")
#
#         dog_details = training_school.get_dog(dog_name)
#
#         assert dog_details["name"] == dog_name
#         assert dog_details["tricks"] == ("roll over", "play dead")
#
#         with ProjectionRunner(
#             application_class=DogSchool,
#             projection_class=CountProjection,
#             view_class=POPOCountRecorder,
#             env=env,
#         ) as runner:
#             # Get "read model" instance from runner, because
#             # state of materialised view is stored in memory.
#             assert isinstance(runner, ProjectionRunner)  # for PyCharm
#             materialised_view = runner.projection.view
#
#             # Wait for the existing events to be processed.
#             materialised_view.wait(
#                 application_name=training_school.name,
#                 notification_id=notification_id,
#                 timeout=100,
#                 interrupt=runner.is_interrupted,
#             )
#
#             # Query the "read model".
#             dog_count = materialised_view.get_dog_counter()
#             trick_count = materialised_view.get_trick_counter()
#
#             # Record another event in "write model".
#             notification_id = training_school.add_trick(dog_name, "sit and stay")
#
#             # Wait for the new event to be processed by the projection.
#             materialised_view.wait(
#                 application_name=training_school.name,
#                 notification_id=notification_id,
#                 timeout=1,
#                 interrupt=runner.is_interrupted,
#             )
#
#             # Expect one trick more, same number of dogs.
#             assert dog_count == materialised_view.get_dog_counter()
#             assert trick_count + 1 == materialised_view.get_trick_counter()
#             # print("Trick count:", trick_count)
#
#             # Write another event.
#             notification_id = training_school.add_trick(dog_name, "jump hoop")
#
#             # Wait for the new event to be processed.
#             materialised_view.wait(
#                 training_school.name,
#                 notification_id,
#             )
#
#             # Expect two tricks more, same number of dogs.
#             assert dog_count == materialised_view.get_dog_counter()
#             assert trick_count + 2 == materialised_view.get_trick_counter()
