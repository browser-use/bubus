"""Test the EventBus.log_tree() method"""

import sys
from datetime import UTC, datetime
from io import StringIO
from typing import Any

from bubus import BaseEvent, EventBus
from bubus.models import EventResult


class RootEvent(BaseEvent[str]):
    data: str = 'root'


class ChildEvent(BaseEvent[list[int]]):
    value: int = 42


class GrandchildEvent(BaseEvent[str]):
    event_result_type: Any = str

    nested: dict[str, int] = {'level': 3}


def test_log_history_tree_empty(capsys: Any) -> None:
    """Test tree output with empty history"""
    bus = EventBus(name='EmptyBus')
    bus.log_tree()

    captured = capsys.readouterr()
    assert 'Event History Tree for EmptyBus' in captured.out
    assert '(No events in history)' in captured.out


def test_log_history_tree_single_event(capsys: Any) -> None:
    """Test tree output with a single event"""
    bus = EventBus(name='SingleBus')

    # Create and add event to history
    event = RootEvent(data='test')
    event.event_processed_at = datetime.now(UTC)
    bus.event_history[event.event_id] = event

    bus.log_tree()

    captured = capsys.readouterr()
    assert '└── ✅ RootEvent#' in captured.out
    # Should show start time and duration
    assert '[' in captured.out and ']' in captured.out


def test_log_history_tree_with_handlers(capsys: Any) -> None:
    """Test tree output with event handlers and results"""
    bus = EventBus(name='HandlerBus')

    # Create event with handler results
    event = RootEvent(data='test')
    event.event_processed_at = datetime.now(UTC)

    # Add handler result
    handler_id = f'{id(bus)}.123456'
    event.event_results[handler_id] = EventResult[str](
        event_id=event.event_id,
        handler_id=handler_id,
        handler_name='test_handler',
        eventbus_id=str(id(bus)),
        eventbus_name='HandlerBus',
        status='completed',
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        result='status: success',
    )

    bus.event_history[event.event_id] = event
    bus.log_tree()

    captured = capsys.readouterr()
    assert '└── ✅ RootEvent#' in captured.out
    assert '└── ✅ HandlerBus.test_handler#' in captured.out
    assert "'status: success'" in captured.out


def test_log_history_tree_with_errors(capsys: Any) -> None:
    """Test tree output with handler errors"""
    bus = EventBus(name='ErrorBus')

    event = RootEvent()
    event.event_processed_at = datetime.now(UTC)

    # Add error result
    handler_id = f'{id(bus)}.789'
    event.event_results[handler_id] = EventResult[str](
        event_id=event.event_id,
        handler_id=handler_id,
        handler_name='error_handler',
        eventbus_id=str(id(bus)),
        eventbus_name='ErrorBus',
        status='error',
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        error=ValueError('Test error message'),
    )

    bus.event_history[event.event_id] = event
    bus.log_tree()

    captured = capsys.readouterr()
    assert '❌ ErrorBus.error_handler#' in captured.out
    assert '❌ ValueError: Test error message' in captured.out


def test_log_history_tree_complex_nested() -> None:
    """Test tree output with complex nested events"""
    bus = EventBus(name='ComplexBus')

    # Create root event
    root = RootEvent(data='root_data')
    root.event_processed_at = datetime.now(UTC)

    # Add root handler with child events
    root_handler_id = f'{id(bus)}.1001'
    root.event_results[root_handler_id] = EventResult[str](
        event_id=root.event_id,
        handler_id=root_handler_id,
        handler_name='root_handler',
        eventbus_id=str(id(bus)),
        eventbus_name='ComplexBus',
        status='completed',
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        result='Root processed',
    )

    # Create child event
    child = ChildEvent(value=100)
    child.event_parent_id = root.event_id
    child.event_processed_at = datetime.now(UTC)

    # Add child to root handler's event_children
    root.event_results[root_handler_id].event_children.append(child)

    # Add child handler with grandchild
    child_handler_id = f'{id(bus)}.2001'
    child.event_results[child_handler_id] = EventResult[list[int]](
        event_id=child.event_id,
        handler_id=child_handler_id,
        handler_name='child_handler',
        eventbus_id=str(id(bus)),
        eventbus_name='ComplexBus',
        status='completed',
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        result=[1, 2, 3],
    )

    # Create grandchild
    grandchild = GrandchildEvent()
    grandchild.event_parent_id = child.event_id
    grandchild.event_processed_at = datetime.now(UTC)

    # Add grandchild to child handler's event_children
    child.event_results[child_handler_id].event_children.append(grandchild)

    # Add grandchild handler
    grandchild_handler_id = f'{id(bus)}.3001'
    grandchild.event_results[grandchild_handler_id] = EventResult[str](
        event_id=grandchild.event_id,
        handler_id=grandchild_handler_id,
        handler_name='grandchild_handler',
        eventbus_id=str(id(bus)),
        eventbus_name='ComplexBus',
        status='completed',
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        result=None,
    )

    # Add all to history
    bus.event_history[root.event_id] = root
    bus.event_history[child.event_id] = child
    bus.event_history[grandchild.event_id] = grandchild

    # Capture output
    captured_output = StringIO()
    sys.stdout = captured_output

    try:
        bus.log_tree()
        output = captured_output.getvalue()
    finally:
        sys.stdout = sys.__stdout__

    # Check structure - note that events may appear both as handler children and in parent mapping
    assert '└── ✅ RootEvent#' in output
    assert '✅ ComplexBus.root_handler#' in output
    assert '✅ ChildEvent#' in output
    assert '✅ ComplexBus.child_handler#' in output
    assert '✅ GrandchildEvent#' in output
    assert '✅ ComplexBus.grandchild_handler#' in output

    # Check result formatting
    assert "'Root processed'" in output
    assert 'list(3 items)' in output
    assert 'None' in output


def test_log_history_tree_multiple_roots(capsys: Any) -> None:
    """Test tree output with multiple root events"""
    bus = EventBus(name='MultiBus')

    # Create multiple root events
    root1 = RootEvent(data='first')
    root1.event_processed_at = datetime.now(UTC)

    root2 = RootEvent(data='second')
    root2.event_processed_at = datetime.now(UTC)

    bus.event_history[root1.event_id] = root1
    bus.event_history[root2.event_id] = root2

    bus.log_tree()

    captured = capsys.readouterr()
    # Both roots should be shown
    assert captured.out.count('├── ✅ RootEvent#') == 1  # First root
    assert captured.out.count('└── ✅ RootEvent#') == 1  # Last root


def test_log_history_tree_timing_info(capsys: Any) -> None:
    """Test that timing information is displayed correctly"""
    bus = EventBus(name='TimingBus')

    event = RootEvent()
    event.event_processed_at = datetime.now(UTC)

    # Add handler with timing
    start_time = datetime.now(UTC)
    end_time = datetime.now(UTC)

    handler_id = f'{id(bus)}.999'
    event.event_results[handler_id] = EventResult[str](
        event_id=event.event_id,
        handler_id=handler_id,
        handler_name='timed_handler',
        eventbus_id=str(id(bus)),
        eventbus_name='TimingBus',
        status='completed',
        started_at=start_time,
        completed_at=end_time,
        result='done',
    )

    bus.event_history[event.event_id] = event
    bus.log_tree()

    captured = capsys.readouterr()
    # Should show timing with duration
    assert '(' in captured.out  # Opening parenthesis for duration
    assert 's)' in captured.out  # Duration in seconds with closing parenthesis


def test_log_history_tree_running_handler(capsys: Any) -> None:
    """Test tree output with handlers still running"""
    bus = EventBus(name='RunningBus')

    event = RootEvent()

    # Add running handler (started but not completed)
    handler_id = f'{id(bus)}.555'
    event.event_results[handler_id] = EventResult[str](
        event_id=event.event_id,
        handler_id=handler_id,
        handler_name='running_handler',
        eventbus_id=str(id(bus)),
        eventbus_name='RunningBus',
        status='started',
        started_at=datetime.now(UTC),
        completed_at=None,
    )

    bus.event_history[event.event_id] = event
    bus.log_tree()

    captured = capsys.readouterr()
    assert '🏃 RunningBus.running_handler#' in captured.out
    assert '🏃 RootEvent#' in captured.out  # Event should also show as running
