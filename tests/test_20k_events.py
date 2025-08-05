import asyncio
import gc
import os
import time
from typing import Any

import psutil
import pytest

from bubus import BaseEvent, EventBus


def get_memory_usage_mb():
    """Get current process memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


class SimpleEvent(BaseEvent):
    """Simple event without Generic for performance testing"""

    pass


@pytest.mark.asyncio
async def test_20k_events_with_memory_control():
    """Test processing 20k events with no memory leaks"""

    # Record initial memory
    gc.collect()
    initial_memory = get_memory_usage_mb()
    print(f'\nInitial memory: {initial_memory:.1f} MB')

    # Create EventBus with proper limits (now default)
    bus = EventBus(name='ManyEvents')

    print('EventBus settings:')
    print(f'  max_history_size: {bus.max_history_size}')
    print(f'  queue maxsize: {bus.event_queue.maxsize if bus.event_queue else "not created"}')
    print('Starting event dispatch...')

    processed_count = 0

    async def handler(event: SimpleEvent) -> None:
        nonlocal processed_count
        processed_count += 1

    bus.on('SimpleEvent', handler)

    total_events = 20_000  # Reduced for faster tests

    start_time = time.time()
    memory_samples: list[float] = []
    max_memory = initial_memory

    # Dispatch all events as fast as possible
    dispatched = 0
    pending_events: list[BaseEvent[Any]] = []

    while dispatched < total_events:
        try:
            event = bus.dispatch(SimpleEvent())
            pending_events.append(event)
            dispatched += 1
            if dispatched <= 5:
                print(f'Dispatched event {dispatched}')
        except RuntimeError as e:
            if 'EventBus at capacity' in str(e):
                # Queue is full, complete the oldest pending events to make room
                # Complete first 10 events to free up space
                if pending_events:
                    to_complete = pending_events[:10]
                    await asyncio.gather(*to_complete)
                    pending_events = pending_events[10:]
            else:
                raise

        # Sample memory every 10k events
        if dispatched % 10_000 == 0 and dispatched > 0:
            gc.collect()
            current_memory = get_memory_usage_mb()
            memory_samples.append(current_memory)
            max_memory = max(max_memory, current_memory)
            elapsed = time.time() - start_time
            rate = dispatched / elapsed
            print(
                f'Progress: {dispatched:,} events, '
                f'Memory: {current_memory:.1f} MB (+{current_memory - initial_memory:.1f} MB), '
                f'History: {len(bus.event_history)}, '
                f'Rate: {rate:.0f} events/sec'
            )

    # Wait for all remaining events to complete
    if pending_events:
        await asyncio.gather(*pending_events)

    # Final wait
    await bus.wait_until_idle()

    duration = time.time() - start_time

    # Final memory check
    gc.collect()
    final_memory = get_memory_usage_mb()
    memory_growth = final_memory - initial_memory
    peak_growth = max_memory - initial_memory

    print('\nFinal Results:')
    print(f'Processed: {processed_count:,} events')
    print(f'Duration: {duration:.2f} seconds')
    print(f'Rate: {processed_count / duration:,.0f} events/sec')
    print(f'Initial memory: {initial_memory:.1f} MB')
    print(f'Peak memory: {max_memory:.1f} MB (+{peak_growth:.1f} MB)')
    print(f'Final memory: {final_memory:.1f} MB (+{memory_growth:.1f} MB)')
    print(f'Event history size: {len(bus.event_history)} (capped at {bus.max_history_size})')

    # Verify results
    assert processed_count == total_events, f'Only processed {processed_count} of {total_events}'
    assert duration < 30, f'Took {duration:.2f}s, should be < 30s'  # Reasonable time for 20k events

    # Check memory usage stayed reasonable
    assert peak_growth < 100, f'Memory grew by {peak_growth:.1f} MB at peak, indicates memory leak'

    # Check event history is properly limited
    assert bus.max_history_size is not None
    assert len(bus.event_history) <= bus.max_history_size, (
        f'Event history has {len(bus.event_history)} events, should be <= {bus.max_history_size}'
    )

    # Explicitly clean up the bus to prevent hanging
    await bus.stop(timeout=0, clear=True)


@pytest.mark.asyncio
async def test_hard_limit_enforcement():
    """Test that hard limit of 100 pending events is enforced"""
    bus = EventBus(name='HardLimitTest')

    try:
        # Create a slow handler to keep events pending
        async def slow_handler(event: SimpleEvent) -> None:
            await asyncio.sleep(0.5)  # Reduced from 10s to 0.5s

        bus.on('SimpleEvent', slow_handler)

        # Try to dispatch more than 100 events
        events_dispatched = 0
        errors = 0

        for _ in range(150):
            try:
                bus.dispatch(SimpleEvent())
                events_dispatched += 1
            except RuntimeError as e:
                if 'EventBus at capacity' in str(e):
                    errors += 1
                else:
                    raise

        print(f'\nDispatched {events_dispatched} events')
        print(f'Hit capacity error {errors} times')

        # Should hit the limit
        assert events_dispatched <= 100
        assert errors > 0

    finally:
        # Properly stop the bus to clean up pending tasks
        await bus.stop(timeout=0, clear=True)  # Don't wait, just force cleanup


@pytest.mark.asyncio
async def test_cleanup_prioritizes_pending():
    """Test that cleanup keeps pending events and removes completed ones"""
    bus = EventBus(name='CleanupTest', max_history_size=10)

    try:
        # Process some events to completion
        completed_events: list[BaseEvent[Any]] = []
        for _ in range(5):
            event = bus.dispatch(BaseEvent(event_type='QuickEvent'))
            completed_events.append(event)

        await asyncio.gather(*completed_events)

        # Add pending events with slow handler (reduced sleep time)
        async def slow_handler(event: BaseEvent) -> None:
            if event.event_type == 'SlowEvent':
                await asyncio.sleep(0.5)  # Reduced from 10s to 0.5s

        bus.on('*', slow_handler)

        pending_events: list[BaseEvent[Any]] = []
        for _ in range(10):
            event = bus.dispatch(BaseEvent(event_type='SlowEvent'))
            pending_events.append(event)

        # Give them time to start
        await asyncio.sleep(0.1)

        # Check history - should prioritize keeping pending events
        history_types: dict[str, int] = {}
        for event in bus.event_history.values():
            status = event.event_status
            history_types[status] = history_types.get(status, 0) + 1

        print('\nHistory after cleanup:')
        print(f'  Total: {len(bus.event_history)} (max: {bus.max_history_size})')
        print(f'  By status: {history_types}')

        # Should have removed completed events to make room for pending
        assert bus.max_history_size is not None
        assert len(bus.event_history) <= bus.max_history_size
        assert history_types.get('pending', 0) + history_types.get('started', 0) >= 5

    finally:
        # Properly stop the bus to clean up pending tasks
        await bus.stop(timeout=0, clear=True)  # Don't wait, just force cleanup
