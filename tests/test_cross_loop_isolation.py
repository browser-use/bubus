# pyright: basic
"""Regression test for cross-loop contamination in BaseEvent.__await__.

When a handler awaits an event, __await__ enters a drain loop that used to iterate
*every* EventBus in the process and run its queued handlers on the awaiting task's
event loop. In a multi-loop application (e.g. several parallel agent sessions, each
on its own loop) that ran one bus's handlers on another bus's loop, where they hung
and piled up until the bus hit its capacity limit:

    RuntimeError: EventBus at capacity: 100 pending events (100 max)

The drain loop must only process buses that belong to the current running loop.
See browser-use/browser-use#5509.
"""

import asyncio
import threading

from bubus import BaseEvent, EventBus


class BlockerEvent(BaseEvent):
    pass


class ProbeEvent(BaseEvent):
    pass


class ParentEvent(BaseEvent):
    pass


def _spin_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def test_await_drain_does_not_process_other_loops_buses():
    loop_a = asyncio.get_running_loop()

    # --- Bus B lives on its own event loop, running in a background thread ---
    loop_b = asyncio.new_event_loop()
    thread = threading.Thread(target=_spin_loop, args=(loop_b,), daemon=True)
    thread.start()

    ran_on: dict[str, int] = {}
    release_blocker = threading.Event()

    async def blocker_handler(event: BlockerEvent) -> None:
        # Occupy bus B's serial processing so its own run loop cannot advance the
        # ProbeEvent. That way the *only* thing that could move the ProbeEvent while
        # the blocker is held is a (buggy) cross-loop drain from loop A.
        while not release_blocker.is_set():
            await asyncio.sleep(0.01)

    async def probe_handler(event: ProbeEvent) -> None:
        ran_on['probe'] = id(asyncio.get_running_loop())

    async def build_bus_b() -> EventBus:
        b = EventBus(name='BusB')
        b.on(BlockerEvent, blocker_handler)
        b.on(ProbeEvent, probe_handler)
        b.dispatch(BlockerEvent())  # starts B's run loop and keeps it busy
        return b

    bus_b = asyncio.run_coroutine_threadsafe(build_bus_b(), loop_b).result(timeout=5)

    # Create the probe on loop B and keep a reference so loop A can await the same
    # object. It sits queued behind the blocker.
    probe = await asyncio.wrap_future(
        asyncio.run_coroutine_threadsafe(_dispatch_probe(bus_b), loop_b)
    )
    await asyncio.sleep(0.1)
    assert bus_b.event_queue is not None and bus_b.event_queue.qsize() >= 1

    # --- Bus A: a handler on loop A awaits bus B's probe event -> enters the drain ---
    bus_a = EventBus(name='BusA')

    async def parent_handler(event: ParentEvent) -> None:
        # Awaiting from inside a handler (holding the global lock) triggers the
        # cross-bus drain loop. Give it a bounded window to (wrongly) pick up the
        # probe from bus B, then stop waiting so the test can assert.
        try:
            await asyncio.wait_for(_await_probe(probe), timeout=0.4)
        except asyncio.TimeoutError:
            pass

    bus_a.on(ParentEvent, parent_handler)
    try:
        await bus_a.dispatch(ParentEvent())

        # The probe belongs to bus B's loop. Loop A must NOT have run it.
        assert ran_on.get('probe') != id(loop_a), (
            "ProbeEvent from bus B ran on bus A's loop — cross-loop contamination"
        )

        # Once bus B is free, its own loop processes the probe — on loop B.
        release_blocker.set()
        for _ in range(100):
            if 'probe' in ran_on:
                break
            await asyncio.sleep(0.02)
        assert ran_on.get('probe') == id(loop_b)
    finally:
        # Always tear down, even if an assertion above fails, so a failing run
        # never leaks bus B's still-spinning run loop or its background thread
        # into later tests.
        release_blocker.set()
        try:
            asyncio.run_coroutine_threadsafe(bus_b.stop(), loop_b).result(timeout=5)
        finally:
            await bus_a.stop()
            loop_b.call_soon_threadsafe(loop_b.stop)
            thread.join(timeout=5)


async def _dispatch_probe(bus: EventBus) -> ProbeEvent:
    return bus.dispatch(ProbeEvent())


async def _await_probe(probe: ProbeEvent) -> None:
    await probe
