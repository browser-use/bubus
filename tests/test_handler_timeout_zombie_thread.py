"""Test that sync handler timeout doesn't hang the event bus.

Option A: sync handlers now run via anyio.to_thread.run_sync with
abandon_on_cancel=True, so the timeout can cancel the await and the bus
stays responsive. The worker thread may still be alive (Python can't kill
threads), but the bus is no longer blocked on it.
"""

import asyncio
import threading

import pytest

from bubus import BaseEvent, EventBus


class BlockEvent(BaseEvent[str]):
    pass


class FastEvent(BaseEvent[str]):
    pass


@pytest.mark.asyncio
async def test_sync_handler_timeout_does_not_block_subsequent_events():
    """A sync handler that blocks must not prevent subsequent events from
    being processed after the timeout fires.

    Before the fix: sync handlers ran directly in the event loop thread,
    so a blocking sync handler blocked the entire loop — the timeout never
    fired and no other events could be processed.

    After the fix: sync handlers run via anyio.to_thread.run_sync, so the
    event loop stays free to enforce the timeout and process other events.
    """
    thread_started = threading.Event()
    thread_should_stop = threading.Event()
    fast_event_handled = threading.Event()

    def blocking_sync_handler(event: BlockEvent) -> str:
        thread_started.set()
        thread_should_stop.wait(timeout=30)
        return "done"

    def fast_sync_handler(event: FastEvent) -> str:
        fast_event_handled.set()
        return "fast"

    bus = EventBus(name="test_sync_timeout")
    bus.on(BlockEvent, blocking_sync_handler)
    bus.on(FastEvent, fast_sync_handler)
    bus._start()

    # Dispatch the blocking event
    bus.dispatch(BlockEvent(event_timeout=1.0))
    try:
        await asyncio.wait_for(bus.step(timeout=1.0), timeout=5.0)
    except (asyncio.TimeoutError, TimeoutError, Exception):
        pass  # Expected — the handler timed out

    # Now dispatch a fast event — it must be processed quickly.
    # Before the fix, the event loop was blocked by the sync handler
    # and this would hang.
    bus.dispatch(FastEvent())
    await asyncio.wait_for(bus.step(timeout=1.0), timeout=5.0)

    thread_should_stop.set()
    await bus.stop(timeout=1, clear=True)

    assert thread_started.is_set(), "Blocking handler should have started"
    assert fast_event_handled.is_set(), "Fast event should have been processed"
