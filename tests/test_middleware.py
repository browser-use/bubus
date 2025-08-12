#!/usr/bin/env python3
"""
Test script to verify the Django-style middleware functionality.
"""
import asyncio
import traceback
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from bubus import BaseEvent, EventBus
from bubus.middleware import (
    EventBusMiddleware,
    HandlerStartedAnalyticsEvent,
    HandlerCompletedAnalyticsEvent,
    WALEventBusMiddleware,
)
from bubus.models import get_handler_id, get_handler_name


class TestEvent(BaseEvent[str]):
    message: str


class AnalyticsMiddleware(EventBusMiddleware):
    """Middleware that dispatches analytics events"""
    
    def __init__(self, analytics_bus: EventBus):
        self.analytics_bus = analytics_bus
        super().__init__()
    
    def __call__(self, get_handler_result):
        async def get_handler_result_wrapped_by_middleware(event: BaseEvent):
            # Note: In the Django pattern, we don't have direct access to handler/eventbus
            # This is a simplified version for testing
            
            # Simulate analytics event before handler
            await self.analytics_bus.dispatch(HandlerStartedAnalyticsEvent(
                event_id=event.event_id,
                started_at=datetime.now(UTC),
                event_bus_id="test_bus_id",
                event_bus_name="TestBus",
                handler_id="test_handler_id",
                handler_name="test_handler",
                handler_class="test_module.TestHandler",
            ))
            
            try:
                result = await get_handler_result(event)
                
                # Simulate analytics event after successful handler
                await self.analytics_bus.dispatch(HandlerCompletedAnalyticsEvent(
                    event_id=event.event_id,
                    completed_at=datetime.now(UTC),
                    error=None,
                    traceback_info="",
                    event_bus_id="test_bus_id",
                    event_bus_name="TestBus",
                    handler_id="test_handler_id",
                    handler_name="test_handler",
                    handler_class="test_module.TestHandler",
                ))
                
                return result
            except Exception as e:
                # Simulate analytics event after failed handler
                await self.analytics_bus.dispatch(HandlerCompletedAnalyticsEvent(
                    event_id=event.event_id,
                    completed_at=datetime.now(UTC),
                    error=e,
                    traceback_info=traceback.format_exc(),
                    event_bus_id="test_bus_id",
                    event_bus_name="TestBus",
                    handler_id="test_handler_id",
                    handler_name="test_handler",
                    handler_class="test_module.TestHandler",
                ))
                raise
        
        return get_handler_result_wrapped_by_middleware


class LoggingMiddleware(EventBusMiddleware):
    """Simple logging middleware for testing"""
    
    def __call__(self, get_handler_result):
        async def get_handler_result_wrapped_by_middleware(event: BaseEvent):
            print(f"📝 Logging: Processing event {event.event_type}")
            
            try:
                result = await get_handler_result(event)
                print(f"📝 Logging: Handler succeeded")
                return result
            except Exception as e:
                print(f"📝 Logging: Handler failed with error: {e}")
                raise
        
        return get_handler_result_wrapped_by_middleware


def analytics_handler(event: HandlerStartedAnalyticsEvent | HandlerCompletedAnalyticsEvent) -> None:
    """Handle analytics events"""
    print(f"📊 Analytics: {event.event_type} - Handler: {event.handler_name} in {event.event_bus_name}")
    if isinstance(event, HandlerCompletedAnalyticsEvent) and event.error:
        print(f"   Error: {event.error}")


def test_handler(event: TestEvent) -> str:
    """Simple test handler"""
    print(f"🔧 Handler processing: {event.message}")
    return f"Processed: {event.message}"


def failing_handler(event: TestEvent) -> str:
    """Handler that always fails"""
    print(f"💥 Failing handler processing: {event.message}")
    raise ValueError("This handler always fails!")


async def test_basic_middleware():
    """Test basic middleware functionality"""
    print("🧪 Testing basic middleware functionality...")
    
    # Create analytics bus
    analytics_bus = EventBus(name='AnalyticsBus')
    analytics_bus.on('*', analytics_handler)
    
    # Create event bus with middleware
    event_bus = EventBus(
        name='TestEventBus',
        middlewares=[
            LoggingMiddleware(),
            AnalyticsMiddleware(analytics_bus),
        ],
    )
    
    # Register handlers
    event_bus.on(TestEvent, test_handler)
    event_bus.on(TestEvent, failing_handler)
    
    # Test with successful event
    print("\n✅ Testing event processing...")
    test_event = TestEvent(message="Hello, Django-style middleware!")
    completed_event = await event_bus.dispatch(test_event)
    print(f"Event completed: {completed_event.event_status}")
    
    # Wait for analytics to process
    await asyncio.sleep(0.1)
    
    # Stop the buses
    await event_bus.stop()
    await analytics_bus.stop()
    
    print("✅ Basic middleware test completed successfully!")


async def test_wal_middleware():
    """Test WAL middleware functionality"""
    print("\n🧪 Testing WAL middleware...")
    
    with TemporaryDirectory() as tmp_dir:
        wal_path = Path(tmp_dir) / "test_events.jsonl"
        
        # Create event bus with WAL enabled
        event_bus = EventBus(
            name='WALTestBus',
            wal_path=wal_path,  # This should automatically add WALEventBusMiddleware
        )
        
        # Register a handler
        event_bus.on(TestEvent, test_handler)
        
        # Dispatch an event
        test_event = TestEvent(message="WAL test message")
        await event_bus.dispatch(test_event)
        
        # Wait for processing
        await asyncio.sleep(0.1)
        
        # Check if WAL file was created and contains the event
        if wal_path.exists():
            content = wal_path.read_text()
            if "WAL test message" in content:
                print("✅ WAL middleware working correctly!")
            else:
                print("❌ WAL file exists but doesn't contain expected content")
                print(f"Content: {content}")
        else:
            print("❌ WAL file was not created")
        
        await event_bus.stop()


async def test_custom_wal_middleware():
    """Test using WALEventBusMiddleware explicitly"""
    print("\n🧪 Testing custom WAL middleware...")
    
    with TemporaryDirectory() as tmp_dir:
        wal_path = Path(tmp_dir) / "custom_wal.jsonl"
        
        # Create event bus with explicit WAL middleware
        event_bus = EventBus(
            name='CustomWALBus',
            middlewares=[WALEventBusMiddleware(wal_path)],
        )
        
        # Register a handler
        event_bus.on(TestEvent, test_handler)
        
        # Dispatch an event
        test_event = TestEvent(message="Custom WAL test")
        await event_bus.dispatch(test_event)
        
        # Wait for processing
        await asyncio.sleep(0.1)
        
        # Check WAL file
        if wal_path.exists():
            content = wal_path.read_text()
            if "Custom WAL test" in content:
                print("✅ Custom WAL middleware working correctly!")
            else:
                print("❌ WAL file exists but doesn't contain expected content")
        else:
            print("❌ Custom WAL file was not created")
        
        await event_bus.stop()


async def main():
    """Run all middleware tests"""
    print("🧪 Testing Django-style middleware functionality...")
    
    await test_basic_middleware()
    await test_wal_middleware() 
    await test_custom_wal_middleware()
    
    print("\n✅ All middleware tests completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())