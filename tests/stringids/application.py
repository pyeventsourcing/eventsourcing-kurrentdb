# from __future__ import annotations
#
# from typing import Any
#
# from eventsourcing.dataclasses import AggregatesApplication
#
# from tests.stringids.domainmodel import Dog, Snapshot
#
#
# class DogSchool(AggregatesApplication):
#     is_snapshotting_enabled = True
#     snapshot_class = Snapshot
#
#     def register_dog(self, name: str) -> int:
#         return self.save(Dog(name))[0].notification.id
#
#     def add_trick(self, dog_name: str, trick: str) -> int:
#         dog: Dog = self.repository.get(Dog.create_id(dog_name))
#         dog.add_trick(trick)
#         return self.save(dog)[0].notification.id
#
#     def get_dog(self, dog_name: str) -> dict[str, Any]:
#         dog: Dog = self.repository.get(Dog.create_id(dog_name))
#         return {"name": dog.name, "tricks": tuple(dog.tricks)}
