from __future__ import annotations

from concurrent.futures.thread import ThreadPoolExecutor
from typing import cast
from uuid import uuid4

import kurrentdbclient.exceptions
from eventsourcing.domain import NIL_UUID, datetime_now_with_tzinfo
from eventsourcing.errors import PersistenceError, ProgrammingError
from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    StoredEvent,
)
from eventsourcing.tests.persistence import (
    AggregateRecorderTestCase,
    ApplicationRecorderTestCase,
)
from kurrentdbclient import KurrentDBClient

from eventsourcing_kurrentdb.recorders import (
    KurrentDBAggregateRecorder,
    KurrentDBApplicationRecorder,
)
from tests.common import INSECURE_CONNECTION_STRING


class TestKurrentDBAggregateRecorder(AggregateRecorderTestCase):
    INITIAL_VERSION = 0
    recorder_supports_idempotent_appends = True

    def setUp(self) -> None:
        self.client = KurrentDBClient(INSECURE_CONNECTION_STRING)

    def create_recorder(self) -> AggregateRecorder:
        return KurrentDBAggregateRecorder(client=self.client)

    def test_insert_and_select(self) -> None:
        # Do the basic checks.
        super().test_insert_and_select()

        # And also carefully check the implementation of gt, lte, limit, desc.
        # Also include metadata, expecting that will be persisted in KurrentDB.

        # Construct the recorder.
        recorder = self.create_recorder()

        # Write three stored events.
        originator_id1 = str(uuid4())
        event1 = StoredEvent(
            originator_id=originator_id1,
            originator_version=self.INITIAL_VERSION,
            topic="topic1",
            state=b'{"state": "state1"}',
            metadata={"user_id": "user-1"},
        )
        event2 = StoredEvent(
            originator_id=originator_id1,
            originator_version=self.INITIAL_VERSION + 1,
            topic="topic2",
            state=b'{"state": "state2"}',
            metadata={"user_id": "user-2"},
        )
        event3 = StoredEvent(
            originator_id=originator_id1,
            originator_version=self.INITIAL_VERSION + 2,
            topic="topic3",
            state=b'{"state": "state3"}',
            metadata={"user_id": "user-3"},
        )

        # Insert three events.
        recorder.insert_events([event1, event2, event3])

        # Select events with gt, lte and limit args.
        self.assert_events_eq(  # reads from after start, limited by limit
            recorder.select_events(originator_id1, gt=0, lte=30, limit=0),
            [],
        )
        self.assert_events_eq(  # reads from after start, limited by limit
            recorder.select_events(originator_id1, gt=0, lte=30, limit=1),
            [event2],
        )
        self.assert_events_eq(  # reads from after start, limited by limit
            recorder.select_events(originator_id1, gt=0, lte=30, limit=2),
            [event2, event3],
        )
        self.assert_events_eq(  # reads from after start, limited by lte
            recorder.select_events(originator_id1, gt=0, lte=0, limit=10),
            [],
        )
        self.assert_events_eq(  # reads from after start, limited by lte
            recorder.select_events(originator_id1, gt=0, lte=1, limit=10),
            [event2],
        )
        self.assert_events_eq(  # reads from after start, limited by lte
            recorder.select_events(originator_id1, gt=0, lte=2, limit=10),
            [event2, event3],
        )
        self.assert_events_eq(  # reads from after start, limited by lte
            recorder.select_events(originator_id1, gt=1, lte=2, limit=10),
            [event3],
        )
        self.assert_events_eq(  # reads from after start, limited by lte
            recorder.select_events(originator_id1, gt=2, lte=10, limit=10),
            [],
        )

        # Select events with lte and limit args.
        self.assert_events_eq(  # read limited by limit
            recorder.select_events(originator_id1, lte=10, limit=1),
            [event1],
        )
        self.assert_events_eq(  # read limited by limit
            recorder.select_events(originator_id1, lte=10, limit=2),
            [event1, event2],
        )
        self.assert_events_eq(  # read limited by lte
            recorder.select_events(originator_id1, lte=0, limit=10),
            [event1],
        )
        self.assert_events_eq(  # read limited by lte
            recorder.select_events(originator_id1, lte=1, limit=10),
            [event1, event2],
        )
        self.assert_events_eq(  # read limited by lte
            recorder.select_events(originator_id1, lte=10, limit=10),
            [event1, event2, event3],
        )
        self.assert_events_eq(  # read limited by both lte and limit
            recorder.select_events(originator_id1, lte=1, limit=1),
            [event1],
        )

        # Select events with desc, gt, lte.
        self.assert_events_eq(  # reads from after end, limited by gt
            recorder.select_events(originator_id1, desc=True, gt=5, lte=10),
            [],
        )
        self.assert_events_eq(  # reads from after end, limited by gt
            recorder.select_events(originator_id1, desc=True, gt=2, lte=10),
            [],
        )
        self.assert_events_eq(  # reads from after end, limited by gt
            recorder.select_events(originator_id1, desc=True, gt=1, lte=10),
            [event3],
        )
        self.assert_events_eq(  # reads from before end, limited by gt
            recorder.select_events(originator_id1, desc=True, gt=1, lte=1),
            [],
        )
        self.assert_events_eq(  # reads from before end, limited by gt
            recorder.select_events(originator_id1, desc=True, gt=0, lte=1),
            [event2],
        )

        # Select events with desc, gt, lte and limit args.
        self.assert_events_eq(  # reads from after end, limited by given limit
            recorder.select_events(originator_id1, desc=True, gt=0, lte=3, limit=1),
            [event3],
        )
        self.assert_events_eq(  # reads from end, limited by given limit
            recorder.select_events(originator_id1, desc=True, gt=0, lte=2, limit=1),
            [event3],
        )
        self.assert_events_eq(  # reads from before end, limited by given limit
            recorder.select_events(originator_id1, desc=True, gt=0, lte=1, limit=1),
            [event2],
        )

        self.assert_events_eq(  # reads from after end, limited by gt
            recorder.select_events(originator_id1, desc=True, gt=0, lte=3, limit=10),
            [event3, event2],
        )
        self.assert_events_eq(  # reads from end, limited by gt
            recorder.select_events(originator_id1, desc=True, gt=0, lte=2, limit=10),
            [event3, event2],
        )
        self.assert_events_eq(  # reads from before end, limited by gt
            recorder.select_events(originator_id1, desc=True, gt=0, lte=1, limit=10),
            [event2],
        )

        self.assert_events_eq(  # reads from after end, limited by gt and limit
            recorder.select_events(originator_id1, desc=True, gt=0, lte=3, limit=2),
            [event3, event2],
        )
        self.assert_events_eq(  # reads from end, limited by gt and limit
            recorder.select_events(originator_id1, desc=True, gt=0, lte=2, limit=2),
            [event3, event2],
        )
        self.assert_events_eq(  # reads from before end, limited by gt and limit
            recorder.select_events(originator_id1, desc=True, gt=0, lte=1, limit=1),
            [event2],
        )

        # Select events with desc, lte (NO STREAM).
        self.assert_events_eq(  # reads from after end, limited by limit
            recorder.select_events(str(uuid4()), desc=True, lte=10, limit=1),
            [],
        )

        # Select events with desc, lte and limit args.
        self.assert_events_eq(  # reads from after end, limited by limit
            recorder.select_events(originator_id1, desc=True, lte=10, limit=1),
            [event3],
        )
        self.assert_events_eq(  # reads from end, limited by limit
            recorder.select_events(originator_id1, desc=True, lte=2, limit=1),
            [event3],
        )
        self.assert_events_eq(  # reads from before end, limited by limit
            recorder.select_events(originator_id1, desc=True, lte=1, limit=1),
            [event2],
        )
        self.assert_events_eq(  # reads from before end, limited by start of stream
            recorder.select_events(originator_id1, desc=True, lte=1, limit=10),
            [event2, event1],
        )

        # Select events with desc, gt
        self.assert_events_eq(  # reads until after end
            recorder.select_events(originator_id1, desc=True, gt=10),
            [],
        )
        self.assert_events_eq(  # reads until end
            recorder.select_events(originator_id1, desc=True, gt=1),
            [event3],
        )
        self.assert_events_eq(  # reads until before end
            recorder.select_events(originator_id1, desc=True, gt=0),
            [event3, event2],
        )

        # Select events with desc, gt (NO STREAM)
        self.assert_events_eq(  # reads until before end
            recorder.select_events(str(uuid4()), desc=True, gt=1),
            [],
        )

        # Select events with desc, gt, limit
        self.assert_events_eq(  # reads until after end, limited by gt
            recorder.select_events(originator_id1, desc=True, gt=10, limit=10),
            [],
        )
        self.assert_events_eq(  # reads until end, limited by gt
            recorder.select_events(originator_id1, desc=True, gt=1, limit=10),
            [event3],
        )
        self.assert_events_eq(  # reads until before end, limited by gt
            recorder.select_events(originator_id1, desc=True, gt=0, limit=10),
            [event3, event2],
        )
        self.assert_events_eq(  # reads until before end, limited by limit
            recorder.select_events(originator_id1, desc=True, gt=0, limit=1),
            [event3],
        )
        self.assert_events_eq(  # reads until before end, limited by gt and limit
            recorder.select_events(originator_id1, desc=True, gt=1, limit=1),
            [event3],
        )
        self.assert_events_eq(  # reads until before end, limited by limit
            recorder.select_events(originator_id1, desc=True, gt=0, limit=2),
            [event3, event2],
        )

        # Can't store events in more than one stream.
        event4 = StoredEvent(
            originator_id=str(uuid4()),
            originator_version=self.INITIAL_VERSION,
            topic="topic4",
            state=b'{"state": "state4"}',
        )
        event5 = StoredEvent(
            originator_id=str(uuid4()),
            originator_version=self.INITIAL_VERSION,
            topic="topic5",
            state=b'{"state": "state5"}',
        )
        with self.assertRaises(ProgrammingError):
            recorder.insert_events([event4, event5])

        # Also check event_id is recovered from client-generated ID.
        self.assertEqual(event1.uuid, NIL_UUID)
        self.assertEqual(event2.uuid, NIL_UUID)
        self.assertEqual(event3.uuid, NIL_UUID)
        events = recorder.select_events(originator_id1)
        self.assertEqual(len(events), 3)
        event_id1 = events[0].uuid
        event_id2 = events[1].uuid
        event_id3 = events[2].uuid
        self.assertNotEqual(event_id1, event_id2)
        self.assertNotEqual(event_id2, event_id3)
        self.assertNotEqual(event_id3, event_id1)
        events = recorder.select_events(originator_id1)
        self.assertEqual(events[0].uuid, event_id1)
        self.assertEqual(events[1].uuid, event_id2)
        self.assertEqual(events[2].uuid, event_id3)

        # Also check event_id can be set.
        event6 = StoredEvent(
            originator_id=str(uuid4()),
            originator_version=self.INITIAL_VERSION,
            topic="topic6",
            state=b'{"state": "state6"}',
            uuid=uuid4(),
        )
        recorder.insert_events([event6])
        events = recorder.select_events(event6.originator_id)
        self.assertEqual(events[0].uuid, event6.uuid)


class TestKurrentDBApplicationRecorder(ApplicationRecorderTestCase):
    INITIAL_VERSION = 0
    EXPECT_CONTIGUOUS_NOTIFICATION_IDS = False

    def setUp(self) -> None:
        self.validate_uuids = False
        # self.original_initial_version = Aggregate.INITIAL_VERSION
        # Aggregate.INITIAL_VERSION = 0
        self.client = KurrentDBClient(INSECURE_CONNECTION_STRING)

    def tearDown(self) -> None:
        del self.client
        # Aggregate.INITIAL_VERSION = self.original_initial_version

    def create_recorder(self) -> ApplicationRecorder:
        recorder = KurrentDBApplicationRecorder(client=self.client)
        recorder.validate_uuids = self.validate_uuids
        return recorder

    def test_insert_select(self) -> None:
        # Don't call super() because it tests method without `start` argument,
        # and here we aren't restarting KurrentDB for each test method, so it
        # doesn't really work.
        # super().test_insert_select()

        # Construct the recorder.
        # self.validate_uuids = True
        recorder = self.create_recorder()

        # Get the current max notification ID.
        max_notification_id1 = recorder.max_notification_id()
        assert isinstance(max_notification_id1, int)

        # Check notifications methods work when there aren't any.
        self.assertEqual(
            recorder.max_notification_id(),
            max_notification_id1,
        )
        self.assertEqual(
            len(
                recorder.select_notifications(
                    start=max_notification_id1,
                    limit=10,
                    inclusive_of_start=False,
                )
            ),
            0,
        )
        self.assertEqual(
            len(
                recorder.select_notifications(
                    start=max_notification_id1,
                    limit=3,
                    topics=["topic1"],
                    inclusive_of_start=False,
                )
            ),
            0,
        )

        self.assertEqual(
            len(
                recorder.select_notifications(
                    start=max_notification_id1, limit=10, inclusive_of_start=False
                )
            ),
            0,
        )

        # Check inserting an empty list gives an empty list of notification IDs.
        notification_ids = recorder.insert_events([])
        self.assertEqual(notification_ids, [])

        # Define three stored events with two different originator IDs.
        originator_id1 = str(uuid4())
        originator_id2 = str(uuid4())

        event1 = StoredEvent(
            originator_id=originator_id1,
            originator_version=self.INITIAL_VERSION,
            topic="topic1",
            state=b'{"state": "state1"}',
            metadata={"user_id": "user-1"},
            uuid=uuid4(),
        )
        event2 = StoredEvent(
            originator_id=originator_id1,
            originator_version=self.INITIAL_VERSION + 1,
            topic="topic2",
            state=b'{"state": "state2"}',
            metadata={"user_id": "user-2"},
            uuid=uuid4(),
        )
        event3 = StoredEvent(
            originator_id=originator_id2,
            originator_version=self.INITIAL_VERSION,
            topic="topic3",
            state=b'{"state": "state3"}',
            metadata={"user_id": "user-3"},
            uuid=uuid4(),
        )

        # Insert two events.
        notification_ids = recorder.insert_events([event1, event2])

        # Check we got two event notification IDs.
        assert notification_ids is not None
        self.assertEqual(len(notification_ids), 2)

        # Check they are larger than the initial max notification ID.
        self.assertGreater(notification_ids[0], max_notification_id1)
        self.assertGreater(notification_ids[1], max_notification_id1)

        # Check the last one is the same as the current max notification ID.
        max_notification_id2 = recorder.max_notification_id()
        assert isinstance(max_notification_id2, int)

        self.assertEqual(notification_ids[-1], max_notification_id2)

        # Insert the third event.
        notification_ids = recorder.insert_events([event3])

        # Check we got one event notification ID.
        assert notification_ids is not None
        self.assertEqual(len(notification_ids), 1)

        # Check it is larger than the last max notification ID.
        self.assertGreater(notification_ids[0], max_notification_id2)

        # Check it is the same as the current max notification ID.
        max_notification_id3 = recorder.max_notification_id()
        assert isinstance(max_notification_id3, int)
        self.assertEqual(notification_ids[0], max_notification_id3)

        # Select the recorded events for each originator ID,
        # and check we got what was written.
        self.assert_events_eq(
            recorder.select_events(originator_id1),
            [event1, event2],
        )
        self.assert_events_eq(
            recorder.select_events(originator_id2),
            [event3],
        )

        # InvalidPosition error when selecting from max_notification_id1 + 1.
        with self.assertRaises(kurrentdbclient.exceptions.InvalidCommitPositionError):
            recorder.select_notifications(
                max_notification_id1 + 1, 10, inclusive_of_start=False
            )
        # From docker.eventstore.com/eventstore-ce/eventstoredb-ce:22.10.4-jammy
        # <_MultiThreadedRendezvous of RPC that terminated with:
        # 	status = StatusCode.UNKNOWN
        # 	details = "Unexpected FilteredReadAllResult: Error"
        # 	debug_error_string = "UNKNOWN:Error received from peer  {created_time:
        # 	"2025-05-07T01:05:37.567805+01:00", grpc_status:2, grpc_message:
        # 	"Unexpected FilteredReadAllResult: Error"}"
        # >

        # From docker.eventstore.com/eventstore/eventstoredb-ee:
        #   24.10.0-x64-8.0-bookworm-slim
        # <_MultiThreadedRendezvous of RPC that terminated with:
        # 	status = StatusCode.UNKNOWN
        # 	details = "Unexpected FilteredReadAllResult: InvalidPosition"
        # 	debug_error_string = "UNKNOWN:Error received from peer  {created_time:
        # 	"2025-05-07T01:00:12.828196+01:00", grpc_status:2, grpc_message:
        # 	"Unexpected FilteredReadAllResult: InvalidPosition"}"
        # >

        notifications = recorder.select_notifications(
            max_notification_id1, 10, inclusive_of_start=False
        )
        self.assert_events_eq(notifications, [event1, event2, event3])
        self.assertEqual(notifications[0].uuid, event1.uuid)
        self.assertEqual(notifications[1].uuid, event2.uuid)
        self.assertEqual(notifications[1].id, max_notification_id2)
        self.assertEqual(notifications[2].uuid, event3.uuid)
        self.assertEqual(notifications[2].id, max_notification_id3)

        # Select notification by topic (all topics).
        notifications = recorder.select_notifications(
            max_notification_id1,
            10,
            topics=["topic1", "topic2", "topic3"],
            inclusive_of_start=False,
        )

        # Check we got three notifications.
        self.assert_events_eq(notifications, [event1, event2, event3])
        # Check the event_id values are getting returned.
        self.assertEqual(notifications[0].uuid, event1.uuid)
        self.assertEqual(notifications[1].uuid, event2.uuid)
        self.assertEqual(notifications[2].uuid, event3.uuid)

        # Select notification by topic (topic1 only).
        self.assert_events_eq(
            recorder.select_notifications(
                max_notification_id1, 10, topics=["topic1"], inclusive_of_start=False
            ),
            [event1],
        )

        # Select notification by topic (topic2 only).
        self.assert_events_eq(
            recorder.select_notifications(
                max_notification_id1, 10, topics=["topic2"], inclusive_of_start=False
            ),
            [event2],
        )

        # Select notification by topic (topic3 only).
        self.assert_events_eq(
            recorder.select_notifications(
                max_notification_id1, 10, topics=["topic3"], inclusive_of_start=False
            ),
            [event3],
        )

        # Select notification by topic (topic1 and topic3 only).
        self.assert_events_eq(
            recorder.select_notifications(
                max_notification_id1,
                10,
                topics=["topic1", "topic3"],
                inclusive_of_start=False,
            ),
            [event1, event3],
        )

        # Select a limited number of notifications from initial position.
        self.assert_events_eq(
            recorder.select_notifications(
                max_notification_id1,
                limit=1,
                inclusive_of_start=False,
            ),
            [event1],
        )

        # Select a limited number of notifications from later position.
        self.assert_events_eq(
            recorder.select_notifications(
                max_notification_id2,
                limit=1,
                inclusive_of_start=False,
            ),
            [event3],
        )

        # Select a limited number of notifications
        # from later position (inclusive of start).
        self.assert_events_eq(
            recorder.select_notifications(
                max_notification_id2,
                limit=1,
                inclusive_of_start=True,
            ),
            [event2],
        )

        # Select notifications between two positions
        self.assert_events_eq(
            recorder.select_notifications(
                start=max_notification_id1,
                limit=10,
                stop=max_notification_id2,
                inclusive_of_start=False,
            ),
            [event1, event2],
        )

        # # Cover exception handling when stream name is not a UUID.
        # max_notification_id4 = recorder.max_notification_id()
        # assert isinstance(max_notification_id4, int)
        #
        # cast(KurrentDBApplicationRecorder, recorder).client.append_to_stream(
        #     stream_name=f"not-a-uuid-{uuid4()}",
        #     events=NewEvent(type="SomethingHappened", data=b"{}"),
        #     current_version=StreamState.NO_STREAM,
        # )
        # with self.assertRaises(ValueError) as cm1:
        #     recorder.select_notifications(
        #         start=max_notification_id4,
        #         limit=10,
        #         stop=max_notification_id2,
        #         inclusive_of_start=False,
        #     )
        # self.assertIn("badly formed hexadecimal UUID string", str(cm1.exception))

        # Cover non-wrong-current-version exception handling when appending events.
        cast(KurrentDBApplicationRecorder, recorder).client.close()
        cast(KurrentDBApplicationRecorder, recorder).client.connection_spec._targets = [
            "127.0.0.1:1000"
        ]
        with self.assertRaises(PersistenceError) as cm2:
            recorder.insert_events([event3])
        self.assertIn("failed to connect", str(cm2.exception))

    def test_concurrent_no_conflicts(self, initial_position: int = 0) -> None:
        super().test_concurrent_no_conflicts(initial_position)

    def test_insert_subscribe(self) -> None:
        # self.validate_uuids = True
        super().optional_test_insert_subscribe()

    def test_subscribe_concurrent_reading_and_writing(self) -> None:
        recorder = self.create_recorder()

        num_batches = 1000
        batch_size = 1
        num_events = num_batches * batch_size

        def read(last_notification_id: int | None) -> None:
            subscription = recorder.subscribe(last_notification_id)
            start = datetime_now_with_tzinfo()
            with subscription:
                for i, _ in enumerate(subscription):
                    # print("Read", i+1, "notifications")
                    # last_notification_id = notification.id
                    if i + 1 == num_events:
                        break
            duration = datetime_now_with_tzinfo() - start
            print(
                f"Finished reading {num_events} events "
                f"in {duration.total_seconds()} seconds"
            )

        def write() -> None:
            start = datetime_now_with_tzinfo()
            for _ in range(num_batches):
                originator_id = str(uuid4())
                events = []
                for i in range(batch_size):
                    event = StoredEvent(
                        originator_id=originator_id,
                        originator_version=i,
                        topic="topic1",
                        state=b'{"state": "state1"}',
                    )
                    events.append(event)
                recorder.insert_events(events)
                # print("Wrote", i + 1, "notifications")
            duration = datetime_now_with_tzinfo() - start
            print(
                f"Finished writing {num_events} events "
                f"in {duration.total_seconds()} seconds"
            )

        thread_pool = ThreadPoolExecutor(max_workers=2)

        print("Concurrent...")
        # Get the max notification ID (for the subscription).
        last_notification_id = recorder.max_notification_id()
        write_job = thread_pool.submit(write)
        read_job = thread_pool.submit(read, last_notification_id)
        write_job.result()
        read_job.result()

        print("Sequential...")
        last_notification_id = recorder.max_notification_id()
        write_job = thread_pool.submit(write)
        write_job.result()
        read_job = thread_pool.submit(read, last_notification_id)
        read_job.result()

        thread_pool.shutdown(wait=True)

    def test_str_originator_ids(self) -> None:
        self.validate_uuids = False
        recorder = self.create_recorder()

        originator_id = f"product-{uuid4()}"
        event = StoredEvent(
            originator_id=originator_id,
            originator_version=0,
            topic="topic1",
            state=b'{"state": "state1"}',
        )

        recorder.insert_events([event])


del AggregateRecorderTestCase
del ApplicationRecorderTestCase
