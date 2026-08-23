"""Test that handler timeout actually frees threads blocked in synchronous code.

When a handler is blocked in synchronous code (e.g. a C-level lock, blocking
I/O, or a long sleep inside ``run_in_executor``), ``asyncio.wait_for`` cancels
the asyncio task but cannot interrupt the underlying blocking call.
``CancelledError`` can only be delivered at an ``await`` point, and the
blocking thread never reaches one.

The ``execute_handler`` ``finally`` block tries to cancel the task but only
waits 0.1s for the cancellation to take effect, then abandons it:

    if handler_task and not handler_task.done():
        handler_task.cancel()
        try:
            await asyncio.wait_for(handler_task, timeout=0.1)  # ← only 0.1s
        except (asyncio.CancelledError, TimeoutError):
            pass  # Expected when we cancel the task

This leaves the thread alive forever — a zombie thread that accumulates until
the thread pool is exhausted.

In production (browser_use / OpenHands agent-server), this manifests when a
browser launch hangs on a C-level lock: the 30s handler timeout fires and
logs "TIMEOUT HERE", but the thread stays stuck. After enough conversations,
21 zombie threads accumulate and new conversation creation stalls.
"""

import asyncio
import threading
import time

import pytest

from bubus import BaseEvent, EventBus


class BlockEvent(BaseEvent[str]):
    """Event whose handler will block in synchronous code."""
    pass


def test_timeout_frees_handler_blocked_in_sync_code():
    """A handler that blocks in synchronous code (via run_in_executor) must
    be freed after the timeout fires, not left as a zombie thread.

    This test fails on the current codebase: the timeout fires after 1s and
    logs the error, but the worker thread stays alive because
    ``asyncio.wait_for`` cancels the Future's await but not the underlying
    thread. The ``finally`` block waits only 0.1s then abandons the task.
    """
    thread_started = threading.Event()
    thread_should_stop = threading.Event()
    zombie_threads_before = [
        t for t in threading.enumerate()
        if t is not threading.main_thread() and t.is_alive() and not t.daemon
    ]

    def _block_sync():
        """Simulate a blocking C-level call (e.g. playwright browser launch)."""
        thread_started.set()
        # Block for 30s — simulates a hung browser launch on a C-level lock.
        # This cannot be interrupted by asyncio cancellation.
        thread_should_stop.wait(timeout=30)
        return "done"

    async def blocking_handler(event: BlockEvent) -> str:
        # run_in_executor returns a Future; cancelling the asyncio task
        # cancels the Future's await but NOT the underlying thread.
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _block_sync)

    bus = EventBus(name="test_timeout_zombie")
    bus.on(BlockEvent, blocking_handler)
    bus._start()

    async def _run():
        # Dispatch with a 1s timeout. The handler will block for 30s.
        bus.dispatch(BlockEvent(event_timeout=1.0))
        await bus.step(timeout=1.0)

    try:
        asyncio.run(asyncio.wait_for(_run(), timeout=5))
    except (asyncio.TimeoutError, TimeoutError, Exception):
        pass  # Expected — the handler timed out

    # Give the cleanup code time to run
    time.sleep(1)

    # Signal the thread to stop so it can exit if it's still alive
    thread_should_stop.set()
    time.sleep(0.5)

    # Stop the bus
    try:
        asyncio.run(bus.stop(timeout=1))
    except Exception:
        pass

    # Verify the handler actually started
    assert thread_started.is_set(), "Handler thread should have started"

    # Count active non-daemon threads (excluding any that existed before the test)
    zombie_threads_after = [
        t for t in threading.enumerate()
        if t is not threading.main_thread() and t.is_alive() and not t.daemon
        and t not in zombie_threads_before
    ]

    # This assertion FAILS — the zombie thread is still alive:
    assert len(zombie_threads_after) == 0, (
        f"Handler timed out but {len(zombie_threads_after)} zombie thread(s) "
        f"are still alive: {[t.name for t in zombie_threads_after]}. "
        f"The timeout fired and logged 'TIMEOUT HERE', but the worker thread "
        f"blocked in synchronous code was never freed. "
        f"asyncio.wait_for cancelled the Future's await but not the underlying "
        f"thread, and the finally block only waited 0.1s before abandoning it."
    )
