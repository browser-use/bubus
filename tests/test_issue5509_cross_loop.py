"""
Reproduction for issue #5509: cross-loop contamination in the EventBus drain loop.

Scenario: two EventBus instances running in parallel (like two concurrent
agent sessions in browser-use). A handler chain on bus A (parent -> child ->
grandchild) enters the inner `__await__` drain loop, which iterates over
`EventBus.all_instances` and processes queued events from ALL buses.

Before the fix, a bus B event queued during that window is processed by the
drain loop — i.e. INSIDE bus A's handler context (while bus A holds the global
lock and bus B's own run loop is starved). This cross-loop execution steals
bus B's scheduling, and with many parallel sessions it is what drives the
EventBus capacity errors reported in the issue.

After the fix, the drain loop only processes events belonging to its own
waiting chain; bus B's independent event stays queued and is handled by bus
B's own run loop once the lock is released.
"""

import asyncio

import pytest

from bubus import BaseEvent, EventBus


class ParentAEvent(BaseEvent[str]):
    message: str


class ChildAEvent(BaseEvent[str]):
    data: str


class GrandchildAEvent(BaseEvent[str]):
    value: int


class BusBEvent(BaseEvent[str]):
    """Independent event that should only be handled on bus B."""

    payload: str


class StartEvent(BaseEvent[str]):
    """Handler-less event used only to auto-start a bus's run loop."""

    data: str


@pytest.fixture
async def buses():
    """Two isolated buses simulating parallel sessions."""
    # Create bus_b first so it appears before bus_a in the
    # `EventBus.all_instances` iteration order (WeakSet keeps insertion
    # order); the drain loop scans buses in that order, so bus B's queued
    # event is hit before the drain completes its own chain and breaks.
    bus_b = EventBus(name="bus_b")
    bus_a = EventBus(name="bus_a")
    # First dispatch auto-starts each bus's run loop (and creates event_queue).
    bus_a.dispatch(StartEvent(data="__start__"))
    bus_b.dispatch(StartEvent(data="__start__"))
    await asyncio.gather(
        bus_a.wait_until_idle(timeout=5),
        bus_b.wait_until_idle(timeout=5),
    )
    yield bus_a, bus_b
    await bus_a.stop(clear=True)
    await bus_b.stop(clear=True)


@pytest.mark.asyncio
async def test_bus_b_event_not_processed_inside_bus_a_drain(buses):
    bus_a, bus_b = buses

    ready = asyncio.Event()
    proceed = asyncio.Event()
    order: list[str] = []

    async def grandchild_handler(event: GrandchildAEvent) -> int:
        await asyncio.sleep(0.5)
        return event.value

    async def child_handler(event: ChildAEvent) -> str:
        # Tell the test the chain is armed; wait until bus B's independent
        # event is queued BEFORE forwarding the grandchild, so bus B's queue
        # holds [independent event, grandchild] when the inner drain loop
        # scans it — the drain must skip the unrelated event and still
        # process the grandchild.
        ready.set()
        await proceed.wait()
        order.append("a_drain_enter")
        grandchild = GrandchildAEvent(value=1)
        bus_b.dispatch(grandchild)
        await grandchild
        order.append("a_drain_exit")
        return "child_handled"

    async def handler_a(event: ParentAEvent) -> str:
        child = ChildAEvent(data=f"child_of_{event.message}")
        bus_a.dispatch(child)
        await child
        return "a_handled"

    async def handler_b(event: BusBEvent) -> str:
        order.append("b_handled")
        return "b_handled"

    bus_a.on("GrandchildAEvent", grandchild_handler)
    bus_a.on("ChildAEvent", child_handler)
    bus_a.on("ParentAEvent", handler_a)
    bus_b.on("BusBEvent", handler_b)

    # Start bus A's event chain; wait until its handler holds the global lock
    # (child_handler is parked on `proceed` inside bus A's run loop).
    bus_a.dispatch(ParentAEvent(message="trigger"))
    await asyncio.wait_for(ready.wait(), timeout=5)

    # Dispatch an event on bus B: its run loop `get()`s it (outside the lock)
    # and then blocks acquiring the global lock held by bus A — so bus B's
    # consumer is parked and will not `get()` again until the lock is free.
    bus_b.dispatch(BusBEvent(payload="first"))
    await asyncio.sleep(0.05)

    # Queue a SECOND independent event on bus B (directly into its queue):
    # with bus B's run loop parked on the lock, this event is only reachable
    # by bus A's drain loop — the cross-loop window (issue #5509).
    bus_b.event_queue.put_nowait(BusBEvent(payload="second"))
    proceed.set()

    await asyncio.gather(
        bus_a.wait_until_idle(timeout=5),
        bus_b.wait_until_idle(timeout=5),
    )

    # Bus B's events must NOT be processed while bus A's drain loop is running
    # (inside bus A's handler context, holding the global lock): that is the
    # cross-loop contamination. Both events must be handled afterwards by bus
    # B's own run loop, once the lock is released.
    assert order[:2] == ["a_drain_enter", "a_drain_exit"], f"drain markers missing: {order!r}"
    assert order[2:] == ["b_handled", "b_handled"], (
        f"cross-loop contamination: bus B events were processed inside bus A's "
        f"drain loop (order was {order!r}, expected both bus B events after the drain)"
    )
