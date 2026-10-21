# pyright: basic
import asyncio

import pytest

from bubus import BaseEvent, EventBus
from bubus.service import QueueShutDown


class PingEvent(BaseEvent):
    pass


@pytest.mark.parametrize('clear', [False, True])
async def test_late_dispatch_does_not_restart_stopped_bus(clear: bool):
    bus = EventBus()
    await bus.dispatch(PingEvent())
    await bus.stop(timeout=1, clear=clear)

    try:
        with pytest.raises(QueueShutDown):
            bus.dispatch(PingEvent())
        assert not bus._is_running
        assert bus._runloop_task is None
    finally:
        await bus.stop()


async def test_cancel_idle_runloop_closes_queue_waiter():
    bus = EventBus()
    await bus.dispatch(PingEvent())
    await bus.wait_until_idle()
    task = bus._runloop_task
    assert task is not None
    assert bus.event_queue is not None

    # Cancel while the loop is waiting for its next event.
    async with asyncio.timeout(1):
        while not bus.event_queue._getters:  # noqa: ASYNC110 - wait for the real queue waiter before cancellation
            await asyncio.sleep(0)

    try:
        task.cancel()
        done, _ = await asyncio.wait({task}, timeout=1)
        assert task in done
        assert not bus._is_running
        assert not bus.event_queue._getters
    finally:
        await bus.stop()


@pytest.mark.parametrize('clear', [False, True])
async def test_wait_until_idle_after_stop_does_not_restart_bus(clear: bool):
    bus = EventBus()
    await bus.dispatch(PingEvent())
    await bus.stop(timeout=1, clear=clear)

    await bus.wait_until_idle(timeout=1)

    assert not bus._is_running
    assert bus._runloop_task is None


@pytest.mark.parametrize('clear', [False, True])
async def test_stop_releases_pending_idle_wait(clear: bool):
    bus = EventBus()
    started, release = asyncio.Event(), asyncio.Event()

    async def handle(event: PingEvent):
        started.set()
        await release.wait()

    bus.on(PingEvent, handle)
    bus.dispatch(PingEvent())
    bus.dispatch(PingEvent())
    await asyncio.wait_for(started.wait(), timeout=1)
    waiter = asyncio.create_task(bus.wait_until_idle())
    await asyncio.sleep(0)
    assert not waiter.done()

    try:
        await bus.stop(timeout=0, clear=clear)
        done, _ = await asyncio.wait({waiter}, timeout=1)
        assert waiter in done
        assert waiter.result() is None
        assert not bus._is_running
    finally:
        release.set()
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
        await bus.stop()


async def test_idle_wait_after_new_dispatch_waits_for_handler():
    bus = EventBus()
    await bus.wait_until_idle()
    started, release = asyncio.Event(), asyncio.Event()

    async def handle(event: PingEvent):
        started.set()
        await release.wait()

    bus.on(PingEvent, handle)
    event = bus.dispatch(PingEvent())
    waiter = asyncio.create_task(bus.wait_until_idle())
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        assert not waiter.done()
        release.set()
        await asyncio.wait_for(waiter, timeout=1)
        assert event.event_status == 'completed'
        assert bus.event_queue is not None
        assert bus.event_queue.empty()
    finally:
        release.set()
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
        await bus.stop()


async def test_queue_shutdown_exits_runloop():
    bus = EventBus()
    await bus.dispatch(PingEvent())
    await bus.wait_until_idle()
    task = bus._runloop_task
    assert task is not None
    assert bus.event_queue is not None

    async with asyncio.timeout(1):
        while not bus.event_queue._getters:  # noqa: ASYNC110 - wait for the real queue waiter before shutdown
            await asyncio.sleep(0)

    try:
        bus.event_queue.shutdown()
        done, _ = await asyncio.wait({task}, timeout=1)
        assert task in done
        assert not bus._is_running
        assert not bus.event_queue._getters
    finally:
        await bus.stop()
