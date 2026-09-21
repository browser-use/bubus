import asyncio

import pytest

from bubus import BaseEvent, EventBus
from bubus.service import EventBusCapacityError


async def test_history_eviction_cannot_hide_outstanding_events():
    bus = EventBus(max_history_size=1)
    admitted = []
    try:
        for _ in range(100):
            event = bus.dispatch(BaseEvent())
            admitted.append(event)
            assert bus.event_queue.get_nowait() is event
            bus.event_queue.task_done()
        assert len(bus.event_history) == 1
        assert len({e.event_id for e in admitted}) == 100
        with pytest.raises(EventBusCapacityError, match='capacity: 100 pending'):
            bus.dispatch(BaseEvent())
        assert len(bus._outstanding_events) == 100
    finally:
        await bus.stop(timeout=0, clear=True)
    assert not bus._outstanding_events


async def test_queue_bound_completion_and_exception_compatibility():
    bus = EventBus(max_history_size=1)
    seen = []

    async def consume(event: BaseEvent):
        seen.append(event.event_id)

    bus.on(BaseEvent, consume)
    try:
        events = [bus.dispatch(BaseEvent()) for _ in range(50)]
        with pytest.raises(EventBusCapacityError, match='capacity: 50 pending') as error:
            bus.dispatch(BaseEvent())
        assert isinstance(error.value, (RuntimeError, asyncio.QueueFull))
        assert bus.event_queue.maxsize == 50
        await bus.wait_until_idle(timeout=5)
        assert sorted(seen) == sorted(e.event_id for e in events)
        assert not bus._outstanding_events
        await bus.dispatch(BaseEvent())
        assert len(seen) == 51
    finally:
        await bus.stop(timeout=2, clear=True)


async def test_started_event_survives_history_eviction_in_capacity_count():
    bus = EventBus(max_history_size=1)
    entered, release = asyncio.Event(), asyncio.Event()
    completed = []

    async def hold(event: BaseEvent):
        entered.set()
        await release.wait()
        completed.append(event.event_id)

    bus.on(BaseEvent, hold)
    try:
        first = bus.dispatch(BaseEvent())
        await entered.wait()
        queued = [bus.dispatch(BaseEvent()) for _ in range(50)]
        assert first.event_id not in bus.event_history
        with pytest.raises(EventBusCapacityError, match='51 pending.*Queue: 50, Processing: 1'):
            bus.dispatch(BaseEvent())
        release.set()
        await bus.wait_until_idle(timeout=5)
        assert sorted(completed) == sorted(e.event_id for e in [first, *queued])
    finally:
        release.set()
        await bus.stop(timeout=2, clear=True)


async def test_distinct_instances_with_same_event_id_remain_counted():
    bus = EventBus(max_history_size=1)
    entered, release = asyncio.Event(), asyncio.Event()
    completed = []

    async def hold(event: BaseEvent):
        entered.set()
        await release.wait()
        completed.append(id(event))

    bus.on(BaseEvent, hold)
    try:
        first = bus.dispatch(BaseEvent())
        await entered.wait()
        second = bus.dispatch(BaseEvent(event_id=first.event_id))
        assert len(bus._outstanding_events) == 2
        release.set()
        await bus.wait_until_idle(timeout=5)
        assert sorted(completed) == sorted([id(first), id(second)])
        assert not bus._outstanding_events
    finally:
        release.set()
        await bus.stop(timeout=2, clear=True)


async def test_repeated_same_instance_preserves_one_handler_execution():
    bus = EventBus()
    calls = []

    async def consume(event: BaseEvent):
        calls.append(id(event))

    bus.on(BaseEvent, consume)
    try:
        event = BaseEvent()
        bus.dispatch(event)
        bus.dispatch(event)
        assert bus.event_queue.qsize() == 2
        await bus.wait_until_idle(timeout=5)
        assert calls == [id(event)]
        assert not bus._outstanding_events
    finally:
        await bus.stop(timeout=2, clear=True)
