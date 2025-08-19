"""Middleware system for event bus with Django-style nested function pattern."""

import asyncio
import traceback
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from bubus.models import BaseEvent, EventHandler, PythonIdStr, get_handler_id, get_handler_name

if TYPE_CHECKING:
    from bubus.service import EventBus


# Type alias for middleware functions
EventMiddleware = Callable[['EventBus', EventHandler, 'BaseEvent[Any]', Callable[[], Awaitable[Any]]], Awaitable[Any]]


class HandlerStartedAnalyticsEvent(BaseEvent[None]):
    """Analytics event dispatched when a handler starts execution"""
    
    event_id: str  # ID of the event being processed
    started_at: datetime
    event_bus_id: str
    event_bus_name: str
    handler_id: str
    handler_name: str
    handler_class: str


class HandlerCompletedAnalyticsEvent(BaseEvent[None]):
    """Analytics event dispatched when a handler completes execution"""
    
    event_id: str  # ID of the event being processed
    completed_at: datetime
    error: Exception | None = None
    traceback_info: str = ''
    event_bus_id: str
    event_bus_name: str
    handler_id: str
    handler_name: str
    handler_class: str


class EventBusMiddleware:
    """Base class for Django-style EventBus middleware"""
    
    def __call__(self, get_handler_result: Callable[['BaseEvent[Any]'], Awaitable[Any]]) -> Callable[['BaseEvent[Any]'], Awaitable[Any]]:
        """
        Django-style middleware pattern.
        
        Args:
            get_handler_result: The next middleware in the chain or the actual handler
            
        Returns:
            Wrapped function that processes events
        """
        async def get_handler_result_wrapped_by_middleware(event: BaseEvent[Any]) -> Any:
            return await get_handler_result(event)
        
        return get_handler_result_wrapped_by_middleware


class WALEventBusMiddleware(EventBusMiddleware):
    """Write-Ahead Logging middleware for persisting events to JSONL files"""
    
    def __init__(self, wal_path: Path | str):
        self.wal_path = Path(wal_path)
    
    def __call__(self, get_handler_result: Callable[['BaseEvent[Any]'], Awaitable[Any]]) -> Callable[['BaseEvent[Any]'], Awaitable[Any]]:
        async def get_handler_result_wrapped_by_middleware(event: BaseEvent[Any]) -> Any:
            # Just execute the handler and log completed events to WAL
            # This is a simplified implementation - the original EventBus did more complex WAL handling
            try:
                result = await get_handler_result(event)
                
                # Log completed event to WAL
                try:
                    self.wal_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Use async I/O if available, otherwise sync
                    try:
                        import anyio
                        async with await anyio.open_file(self.wal_path, 'a', encoding='utf-8') as f:
                            await f.write(event.model_dump_json() + '\n')
                    except ImportError:
                        # Fallback to sync I/O
                        with open(self.wal_path, 'a', encoding='utf-8') as f:
                            f.write(event.model_dump_json() + '\n')
                except Exception:
                    # Don't let WAL errors break the handler
                    pass
                
                return result
            except Exception:
                # Could log error events here too, but keeping it simple
                raise
        
        return get_handler_result_wrapped_by_middleware


class AnalyticsEventBusMiddleware(EventBusMiddleware):
    """Analytics middleware that dispatches analytics events for handler execution"""
    
    def __init__(self, analytics_bus: 'EventBus'):
        self.analytics_bus = analytics_bus
    
    def __call__(self, get_handler_result: Callable[['BaseEvent[Any]'], Awaitable[Any]]) -> Callable[['BaseEvent[Any]'], Awaitable[Any]]:
        async def get_handler_result_wrapped_by_middleware(event: BaseEvent[Any]) -> Any:
            # Access event bus and handler info from the event context
            from bubus.models import get_handler_id, get_handler_name
            from bubus.service import _current_handler_id_context, inside_handler_context
            
            # We can access the event bus through event.event_bus
            event_bus = event.event_bus
            
            # Get handler information from context 
            handler_id = _current_handler_id_context.get()
            
            # Get the event result object which contains handler information
            event_result = None
            if handler_id and handler_id in event.event_results:
                event_result = event.event_results[handler_id]
            
            # Dispatch started analytics event if we have the context
            if event_result and inside_handler_context.get():
                started_event = HandlerStartedAnalyticsEvent(
                    event_id=event.event_id,
                    started_at=event_result.started_at or datetime.now(UTC),
                    event_bus_id=event_bus.id,
                    event_bus_name=event_bus.name,
                    handler_id=handler_id,
                    handler_name=event_result.handler_name,
                    handler_class=event_result.handler_class,
                )
                self.analytics_bus.dispatch(started_event)
            
            try:
                result = await get_handler_result(event)
                
                # Dispatch completed analytics event
                if event_result and inside_handler_context.get():
                    completed_event = HandlerCompletedAnalyticsEvent(
                        event_id=event.event_id,
                        completed_at=datetime.now(UTC),
                        error=None,
                        traceback_info='',
                        event_bus_id=event_bus.id,
                        event_bus_name=event_bus.name,
                        handler_id=handler_id,
                        handler_name=event_result.handler_name,
                        handler_class=event_result.handler_class,
                    )
                    self.analytics_bus.dispatch(completed_event)
                
                return result
            except Exception as e:
                # Dispatch completed analytics event with error
                if event_result and inside_handler_context.get():
                    completed_event = HandlerCompletedAnalyticsEvent(
                        event_id=event.event_id,
                        completed_at=datetime.now(UTC),
                        error=e,
                        traceback_info=traceback.format_exc(),
                        event_bus_id=event_bus.id,
                        event_bus_name=event_bus.name,
                        handler_id=handler_id,
                        handler_name=event_result.handler_name,
                        handler_class=event_result.handler_class,
                    )
                    self.analytics_bus.dispatch(completed_event)
                raise
        
        return get_handler_result_wrapped_by_middleware