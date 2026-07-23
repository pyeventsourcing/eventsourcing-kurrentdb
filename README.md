Please note: following the rebranding of EventStoreDB to KurrentDB, this package is
the rebranding of [`eventsourcing-eventstoredb`](https://pypi.org/project/eventsourcing-eventstoredb). Please
migrate your code to use the [`eventsourcing-kurrentdb`](https://pypi.org/project/eventsourcing-kurrentdb)
package when you are ready.

# Event Sourcing in Python with KurrentDB

This is an extension package for the Python [eventsourcing](https://github.com/pyeventsourcing/eventsourcing) library
that provides a persistence module for [KurrentDB and EventStoreDB](https://www.kurrent.io).
It uses the [kurrentdbclient](https://github.com/pyeventsourcing/kurrentdbclient)
package to communicate with KurrentDB via the gRPC interface. It is tested with
KurrentDB 25.1, 26.0, 26.1, across Python versions 3.11 to 3.14.

## Installation

Use pip to install the [stable distribution](https://pypi.org/project/eventsourcing-kurrentdb/)
from the Python Package Index.

    $ pip install eventsourcing-kurrentdb

Please note, it is recommended to install Python packages into a Python virtual environment.

## Getting started

Define aggregates and applications in the usual way. Please note, "streams"
in KurrentDB are constrained to start from position `0`, and this
package expects the `originator_version` of the first event in an aggregate sequence
to be `0`, so you must set `INITIAL_VERSION` on your aggregate classes to `0`.

```python
from typing import TypedDict, Any
from uuid import uuid4

from eventsourcing.domain import event
from eventsourcing.pydantic import Aggregate, AggregatesApplication, Decision


# Event model, expressed as pure business decision.

class DogRegistered(Decision):
    name: str


class TrickAdded(Decision):
    trick: str


# Decision model, expressed as an event-sourced aggregate.

class Dog(Aggregate):
    INITIAL_VERSION = 0  # for KurrentDB

    @staticmethod
    def create_id() -> str:
        return f"dog-{uuid4()!s}"

    @event(DogRegistered)
    def __init__(self, name: str) -> None:
        self.name = name
        self.tricks: list[str] = []

    @event(TrickAdded)
    def add_trick(self, trick: str) -> None:
        self.tricks.append(trick)


# Command and query handlers, expressed as module-level functions.

def register_dog(app: AggregatesApplication, name: str) -> tuple[str, int]:
    dog = Dog(name=name)
    recordings = app.save(dog)
    return (dog.id, recordings[-1].notification.id)

def add_trick(app: AggregatesApplication, dog_id: str, trick: str) -> int:
    dog = app.repository.get(dog_id, Dog)
    dog.add_trick(trick)
    recordings = app.save(dog)
    return recordings[-1].notification.id

def get_dog_details(app: AggregatesApplication, dog_id: str) -> DogDetails:
    dog = app.repository.get(dog_id, Dog)
    return {"name": dog.name, "tricks": tuple(dog.tricks)}


class DogDetails(TypedDict):
    name: str
    tricks: tuple[str, ...]


# Binding to infrastructure, expressing the name of the bounded context.

class TrainingSchool(AggregatesApplication):
    name = "training_school"
```

In this example, the commands and queries are defined with module-level functions. If
you prefer, you can nest the functions under the application class as object methods,
or alternatively define command handler and query handler classes.

## Configuring the application to use KurrentDB

We need to configure the application infrastructure to use KurrentDB.

You can configure an application with environment variables by setting them in the
operating system environment, or by using the application constructor argument `env`,
or by setting the application class attribute `env`.

Set `PERSISTENCE_MODULE` to `'eventsourcing_kurrentdb'`. Also set `KURRENTDB_URI` to a
KurrentDB connection  string URI. This value will be used as the `uri` argument when
the `KurrentDBClient` class is constructed by this package.

```python
kurrentdb_env = {
    "TRAINING_SCHOOL_PERSISTENCE_MODULE": "eventsourcing_kurrentdb",
    "TRAINING_SCHOOL_KURRENTDB_URI": "esdb://localhost:2113?Tls=false",
}

```

If you are connecting to a "secure" KurrentDB server, and if
the root certificate of the certificate authority used to generate the
server's certificate is not installed locally, then also set environment
variable `KURRENTDB_ROOT_CERTIFICATES` to an SSL/TLS certificate
suitable for making a secure gRPC connection to the KurrentDB server(s).
This value will be used as the `root_certificates` argument when the
`KurrentDBClient` class is constructed by this package.

```python
kurrentdb_env[
    "TRAINING_SCHOOL_KURRENTDB_ROOT_CERTIFICATES"
] = "<PEM encoded SSL/TLS root certificates>"
```

Please refer to the [kurrentdbclient](https://github.com/pyeventsourcing/kurrentdbclient)
documentation for details about starting a "secure" or "insecure" KurrentDB
server, and the "kdb" and "kdb+discover" KurrentDB connection string
URI schemes, and how to obtain a suitable SSL/TLS certificate for use
in the client when connecting to a "secure" KurrentDB server.

After configuring environment variables, construct the application.

```python
app = TrainingSchool(kurrentdb_env)
```

Call application methods from tests and user interfaces. The returned
`commit_position` values can be used to wait for eventually-consistent
read models.

```python
(dog_id, commit_position) = register_dog(app, "Fido")
commit_position = add_trick(app, dog_id, "roll over")
commit_position = add_trick(app, dog_id, "play dead")

dog_details = get_dog_details(app, dog_id)
assert dog_details["name"] == "Fido"
assert dog_details["tricks"] == ("roll over", "play dead")
```

To check the events have been durably saved in KurrentDB, rather then just in
the application Python object, we can construct another instance of the application
and get Fido's details again.

```python
app = TrainingSchool(kurrentdb_env)

dog_details = get_dog_details(app, dog_id)

assert dog_details["name"] == "Fido"
assert dog_details["tricks"] == ("roll over", "play dead")
```

## Eventually-consistent materialised views

To project the state of an event-sourced application "write model" into a
materialised view "read model", first define an interface for the materialised view
using the `TrackingRecorder` class from the `eventsourcing` library.

The example below defines methods to count dogs and tricks for the `TrainingSchool`
application

```python
from abc import abstractmethod

from eventsourcing.persistence import Tracking, TrackingRecorder


class MaterialisedViewInterface(TrackingRecorder):
    @abstractmethod
    def incr_dog_counter(self, tracking: Tracking) -> None:
        pass

    @abstractmethod
    def incr_trick_counter(self, tracking: Tracking) -> None:
        pass

    @abstractmethod
    def get_dog_counter(self) -> int:
        pass

    @abstractmethod
    def get_trick_counter(self) -> int:
        pass
```

The `MaterialisedViewInterface` can be implemented as a concrete view class using a durable database such as PostgreSQL.

The example below counts dogs and tricks in memory, using "plain old Python objects".

```python
from eventsourcing.popo import POPOTrackingRecorder


class InMemoryMaterialiseView(POPOTrackingRecorder, MaterialisedViewInterface):
    def __init__(self) -> None:
        super().__init__()
        self._dog_counter = 0
        self._trick_counter = 0

    def incr_dog_counter(self, tracking: Tracking) -> None:
        with self._database_lock:
            self._assert_tracking_uniqueness(tracking)
            self._insert_tracking(tracking)
            self._dog_counter += 1

    def incr_trick_counter(self, tracking: Tracking) -> None:
        with self._database_lock:
            self._assert_tracking_uniqueness(tracking)
            self._insert_tracking(tracking)
            self._trick_counter += 1

    def get_dog_counter(self) -> int:
        return self._dog_counter

    def get_trick_counter(self) -> int:
        return self._trick_counter
```

Define how events will be processed using the `Projection` class from the `eventsourcing` library.

The example below processes `Dog` events. The `DogRegistered` events are processed
by calling `incr_dog_counter()` on the materialised view. The `TrickAdded` events
are processed by calling `incr_trick_counter()`.

```python
from typing import Any

from eventsourcing.projection import Projection
from eventsourcing.utils import get_topic


class CountProjection(Projection[MaterialisedViewInterface]):
    topics = (
        get_topic(DogRegistered),
        get_topic(TrickAdded),
    )

    def process_event(self, envelope: Any, tracking: Tracking) -> None:
        match envelope.decision:
            case DogRegistered():
                self.view.incr_dog_counter(tracking)
            case TrickAdded():
                self.view.incr_trick_counter(tracking)
```

Run the projection with the `ProjectionRunner` class from the `eventsourcing` library.

The example below shows that when the projection is run, the materialised view is updated
by processing the event of the upstream event-sourced `TrainingSchool` application. It
also shows that when tricks are subsequently added to the application's aggregates,
events continue to be processed, such that the trick counter is incremented in the
downstream materialised view "read model".

```python
import os
from eventsourcing.projection import ProjectionRunner

with ProjectionRunner(
    application_class=TrainingSchool,
    projection_class=CountProjection,
    view_class=InMemoryMaterialiseView,
    env=kurrentdb_env,
) as runner:

    # Get "read model" instance from runner, because
    # state of materialised view is stored in memory.
    materialised_view = runner.projection.view

    # Wait for the existing events to be processed.
    materialised_view.wait(
        application_name=app.name,
        notification_id=commit_position,
        timeout=5.0,
    )

    # Query the "read model".
    dog_count = materialised_view.get_dog_counter()
    trick_count = materialised_view.get_trick_counter()

    # Record another event in "write model".
    commit_position = add_trick(app, dog_id, "sit and stay")

    # Wait for the new event to be processed.
    materialised_view.wait(
        application_name=app.name,
        notification_id=commit_position,
        timeout=5.0,
    )

    # Expect one trick more, same number of dogs.
    assert dog_count == materialised_view.get_dog_counter()
    assert trick_count + 1 == materialised_view.get_trick_counter()

    # Write another event.
    commit_position = add_trick(app, dog_id, "jump hoop")

    # Wait for the new event to be processed.
    materialised_view.wait(
        application_name=app.name,
        notification_id=commit_position,
        timeout=5.0,
    )

    # Expect two tricks more, same number of dogs.
    assert dog_count == materialised_view.get_dog_counter()
    assert trick_count + 2 == materialised_view.get_trick_counter()
```

See the Python `eventsourcing` package documentation for more information about
projecting the state of an event-sourced application into materialised views
that use a durable database such as SQLite and PostgreSQL.

## More information

For more information, please refer to the Python
[eventsourcing](https://github.com/pyeventsourcing/eventsourcing) library, the
Python [kurrentdbclient](https://github.com/pyeventsourcing/kurrentdbclient) package,
and the [KurrentDB](https://www.kurrent.io) website.

## Contributors

Clone the GitHub repo and the use the following `make` commands.

Install Poetry.

    $ make install-poetry

Install packages.

    $ make install

Start UmaDB.

    $ make start-kurrentdb

Run tests.

    $ make test

Stop UmaDB.

    $ make stop-kurrentdb

Check the formatting of the code.

    $ make lint

Reformat the code.

    $ make fmt

Tests belong in `./tests`.

Edit package dependencies in `pyproject.toml`. Update installed packages (and the
`poetry.lock` file) using the following command.

    $ make update
