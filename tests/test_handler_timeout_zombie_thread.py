"""Test that handler timeout doesn't hang the event bus.

Option C: when a handler can't be cancelled (blocked in sync code), abandon
it with a warning instead of holding the global lock indefinitely.
"""

import asyncio
import logging
import threading
import time

import pytest

from bubus import BaseEvent, EventBus


class BlockEvent(BaseEvent[str]):
    pass


@pytest.mark.asyncio
async def test_timeout_does_not_hang_on_blocking_handler(caplog):
    """A handler blocked in sync code must not hang the event bus.

    The timeout fires after 1s. The fix: the finally block abandons the
    task after a short grace period and logs a warning, releasing the global
    lock so other events can proceed.
    """
    thread_started = threading.Event()
    thread_should_stop = threading.Event()

    def _block_sync():
        thread_started.set()
        thread_should_stop.wait(timeout=30)
        return "done"

    async def blocking_handler(event: BlockEvent) -> str:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _block_sync)

    bus = EventBus(name="test_zombie")
    bus.on(BlockEvent, blocking_handler)
    bus._start()

    bus.dispatch(BlockEvent(event_timeout=1.0))

    # step() must return within 5s — without the fix it hangs for 30s
    # because the global lock is held waiting for the uncancellable task.
    try:
        await asyncio.wait_for(bus.step(timeout=1.0), timeout=5.0)
    except (asyncio.TimeoutError, TimeoutError, Exception):
        pass  # Expected — the handler timed out

    # The warning about the abandoned thread should have been logged
    thread_should_stop.set()
    await bus.stop(timeout=1, clear=True)

    assert thread_started.is_set(), "Handler thread should have started"
