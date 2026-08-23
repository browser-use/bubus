"""Test that handler timeout doesn't leave zombie threads.

When a handler blocks in synchronous code (via run_in_executor or a C-level
call), CancelledError can't be delivered. The finally block in
execute_handler waits only 0.1s then abandons the task, leaving a zombie
thread that accumulates until thread pool exhaustion.

This test fails on main: 1 zombie thread remains after the timeout fires.
"""

import asyncio
import threading
import time

import pytest

from bubus import BaseEvent, EventBus


class BlockEvent(BaseEvent[str]):
    pass


@pytest.mark.asyncio
async def test_timeout_does_not_leave_zombie_thread():
    """A handler blocked in sync code must not leave a zombie thread after
    the timeout fires and cleanup runs."""
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
    try:
        await asyncio.wait_for(bus.step(timeout=1.0), timeout=5.0)
    except (asyncio.TimeoutError, TimeoutError, Exception):
        pass  # Expected — the handler timed out

    # Signal the thread to stop and wait for it to exit
    thread_should_stop.set()
    time.sleep(1)
    await bus.stop(timeout=1, clear=True)

    assert thread_started.is_set(), "Handler thread should have started"

    # No zombie threads should remain
    zombies = [
        t for t in threading.enumerate()
        if t is not threading.main_thread() and t.is_alive() and not t.daemon
    ]
    assert len(zombies) == 0, (
        f"{len(zombies)} zombie thread(s) still alive after timeout + cleanup: "
        f"{[t.name for t in zombies]}"
    )
