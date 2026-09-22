"""Tests for EventBus.remove() handler unsubscription."""

from bubus import BaseEvent, EventBus


class PingEvent(BaseEvent[str]):
    """Minimal event used to verify handler removal."""


async def test_remove_handler_by_event_class_stops_delivery():
    """Removing a handler registered by event class stops it from receiving future events."""
    bus = EventBus(name='remove_class')
    calls: list[str] = []

    async def handler(event: PingEvent) -> str:
        calls.append(event.event_type)
        return 'ok'

    bus.on(PingEvent, handler)
    assert bus.remove(PingEvent, handler) is True
    assert bus.remove(PingEvent, handler) is False  # second removal is a no-op

    await bus.dispatch(PingEvent())
    await bus.wait_until_idle()

    assert calls == []
    await bus.stop()


async def test_remove_handler_by_string_name():
    """Removing a handler registered by event type string works too."""
    bus = EventBus(name='remove_string')
    calls: list[str] = []

    async def handler(event: BaseEvent) -> str:
        calls.append(event.event_type)
        return 'ok'

    bus.on('PingEvent', handler)
    assert bus.remove('PingEvent', handler) is True
    assert bus.remove('PingEvent', handler) is False

    await bus.dispatch(PingEvent())
    await bus.wait_until_idle()

    assert calls == []
    await bus.stop()


async def test_remove_only_targets_specific_handler():
    """Removing one handler leaves other handlers for the same event intact."""
    bus = EventBus(name='remove_specific')
    calls: list[str] = []

    async def keep(event: PingEvent) -> str:
        calls.append('keep')
        return 'keep'

    async def drop(event: PingEvent) -> str:
        calls.append('drop')
        return 'drop'

    bus.on(PingEvent, keep)
    bus.on(PingEvent, drop)
    assert bus.remove(PingEvent, drop) is True

    await bus.dispatch(PingEvent())
    await bus.wait_until_idle()

    assert calls == ['keep']
    await bus.stop()


async def test_remove_wildcard_handler():
    """Removing a '*' wildcard handler stops it from receiving all events."""
    bus = EventBus(name='remove_wildcard')
    calls: list[str] = []

    async def handler(event: BaseEvent) -> str:
        calls.append(event.event_type)
        return 'ok'

    bus.on('*', handler)
    assert bus.remove('*', handler) is True

    await bus.dispatch(PingEvent())
    await bus.wait_until_idle()

    assert calls == []
    await bus.stop()
