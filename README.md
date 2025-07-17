# `bubus`: 📢 Production-ready event bus library for Python

Bubus is a fully-featured, Pydantic-powered event bus library for async Python.

It's designed for quickly building event-driven applications with Python in a way that "just works" with async support, proper support for nested events, and real concurrency control.

It provides a [pydantic](https://docs.pydantic.dev/latest/)-based API for implementing publish-subscribe patterns with type safety, async/sync handler support, and advanced features like event forwarding between buses. It's inspired by the simplicity of `JS`'s async system and DOM events APIs, and it aims to bring a fully type-checked [`EventTarget`](https://developer.mozilla.org/en-US/docs/Web/API/EventTarget)-style API to Python.

## 🔢 Quickstart

Install bubus and get started with a simple event-driven application:

```bash
pip install bubus
```

```python
import asyncio
from bubus import EventBus, BaseEvent
from your_auth_events import AuthRequestEvent, AuthResponseEvent

bus = EventBus()

class UserLoginEvent(BaseEvent):
    username: str
    timestamp: float

async def handle_login(login_event: UserLoginEvent):
    auth_request = await bus.dispatch(AuthRequestEvent(...))  # dispatch a nested event
    auth_response = await bus.expect(AuthResponseEvent)       # wait for an event to be seen

    print(f"User {event.username} logged in at {event.timestamp} with API key: {await auth_response.event_result()}")
    return {"status": "success", "user": event.username}


bus.on(UserLoginEvent, handle_login)

event = bus.dispatch(UserLoginEvent(username="alice", timestamp=1234567890))
result = await event
print(f"Login handled: {result.event_results}")
```

<br/>

---

<br/>

## ✨ Features

### 🔠 Type-Safe Events with Pydantic

Define events as Pydantic models with full type checking and validation:

```python
from typing import Any
from bubus import BaseEvent

class OrderCreatedEvent(BaseEvent):
    order_id: str
    customer_id: str
    total_amount: float
    items: list[dict[str, Any]]

# Events are automatically validated
event = OrderCreatedEvent(
    order_id="ORD-123",
    customer_id="CUST-456", 
    total_amount=99.99,
    items=[{"sku": "ITEM-1", "quantity": 2}]
)
```

### 🔀 Async and Sync Handler Support

Register both synchronous and asynchronous handlers for maximum flexibility:

```python
# Async handler
async def async_handler(event: BaseEvent):
    await asyncio.sleep(0.1)  # Simulate async work
    return "async result"

# Sync handler
def sync_handler(event: BaseEvent):
    return "sync result"

bus.on('MyEvent', async_handler)
bus.on('MyEvent', sync_handler)
```

Handlers can also be defined under classes for easier organization:

```python
class SomeService:
    some_value = 'this works'

    async def handlers_can_be_methods(self, event: BaseEvent):
        return self.some_value
    
    @classmethod
    async def handler_can_be_classmethods(cls, event: BaseEvent):
        return cls.some_value

    @staticmethod
    async def handlers_can_be_staticmethods(event: BaseEvent):
        return 'this works too'

# All usage patterns behave the same:
bus.on('MyEvent', SomeClass().handlers_can_be_methods)
bus.on('MyEvent', SomeClass.handler_can_be_classmethods)
bus.on('MyEvent', SomeClass.handlers_can_be_staticmethods)
```

### 🔎 Event Pattern Matching

Subscribe to events using multiple patterns:

```python
# By event type string
bus.on('UserActionEvent', handler)

# By event model class
bus.on(UserActionEvent, handler)

# Wildcard - handle all events
bus.on('*', universal_handler)
```

### ⏩ Forward `Events` Between `EventBus`s 

You can define separate `EventBus` instances in different "microservices" to separate different areas of concern.
`EventBus`s can be set up to forward events between each other (with automatic loop prevention):

```python
# Create a hierarchy of buses
main_bus = EventBus(name='MainBus')
auth_bus = EventBus(name='AuthBus')
data_bus = EventBus(name='DataBus')

# Forward events between buses (infinite loops are automatically prevented)
main_bus.on('AuthEvent', auth_bus.dispatch)
auth_bus.on('*', data_bus.dispatch)

# Events flow through the hierarchy with tracking
event = main_bus.dispatch(MyEvent())
await event
print(event.event_path)  # ['MainBus', 'AuthBus', 'DataBus']  # list of buses that have already procssed the event
```

### 🔱 Event Results Aggregation

Collect and aggregate results from multiple handlers:

```python
async def load_user_config(event):
    return {"debug": True, "port": 8080}

async def load_system_config(event):
    return {"debug": False, "timeout": 30}

bus.on('GetConfig', load_user_config)
bus.on('GetConfig', load_system_config)

# Get a merger of all dict results
event = await bus.dispatch(GetConfig())
config = await event.event_results_flat_dict()
# {'debug': False, 'port': 8080, 'timeout': 30}

# Or get individual results
results = await event.event_results_by_handler_id()
```

### 🚦 FIFO Event Processing

Events are processed in strict FIFO order, maintaining consistency:

```python
# Events are processed in the order they were dispatched
for i in range(10):
    bus.dispatch(ProcessTaskEvent(task_id=i))

# Even with async handlers, order is preserved
await bus.wait_until_idle()
```

If a handler dispatches and awaits any child events during execution, those events will jump the FIFO queue and be processed immediately:
```python
def child_handler(event: SomeOtherEvent):
    return 'xzy123'

def main_handler(event: MainEvent):
    # enqueue event for processing after main_handler exits
    child_event = bus.dispatch(SomeOtherEvent())
    
    # can also await child events to process immediately instead of adding to FIFO queue
    completed_child_event = await child_event
    return f'result from awaiting child event: {await completed_child_event.event_result()}'  # 'xyz123'

bus.on(SomeOtherEvent, child_handler)
bus.on(MainEvent, main_handler)

await bus.dispatch(MainEvent()).event_result()
# result from awaiting child event: xyz123
```

### 🧹 Memory Management

EventBus includes automatic memory management to prevent unbounded growth in long-running applications:

```python
# Create a bus with memory limits (default: 50 events)
bus = EventBus(max_history_size=100)  # Keep max 100 events in history

# Or disable memory limits for unlimited history
bus = EventBus(max_history_size=None)
```

**Automatic Cleanup:**
- When `max_history_size` is set, EventBus automatically removes old events when the limit is exceeded
- Completed events are removed first (oldest first), then started events, then pending events
- This ensures active events are preserved while cleaning up old completed events

**Manual Memory Management:**
```python
# For request-scoped buses (e.g. web servers), clear all memory after each request
try:
    event_service = EventService()  # Creates internal EventBus
    await event_service.process_request()
finally:
    # Clear all event history and remove from global tracking
    await event_service.eventbus.stop(clear=True)
```

**Memory Monitoring:**
- EventBus automatically monitors total memory usage across all instances
- Warnings are logged when total memory exceeds 50MB
- Use `bus.stop(clear=True)` to completely free memory for unused buses
- To avoid memory leaks from big events, the default limits are intentionally kept low. events are normally processed as they come in, and there is rarely a need to keep every event in memory longer after its complete. long-term storage should be accomplished using other mechanisms, like the WAL

### ⛓️ Parallel Handler Execution

Enable parallel processing of handlers for better performance.  
The tradeoff is slightly less deterministic ordering as handler execution order will not be guaranteed when run in parallel.

```python
# Create bus with parallel handler execution
bus = EventBus(parallel_handlers=True)

# Multiple handlers run concurrently for each event
bus.on('DataEvent', slow_handler_1)  # Takes 1 second
bus.on('DataEvent', slow_handler_2)  # Takes 1 second

start = time.time()
await bus.dispatch(DataEvent())
# Total time: ~1 second (not 2)
```

### 🪆 Dispatch Nested Child Events From Handlers

Automatically track event relationships and causality tree:

```python
async def parent_handler(event: BaseEvent):
    # handlers can emit more events to be processed asynchronously after this handler completes
    child = ChildEvent()
    child_event_async = event.event_bus.dispatch(child)   # equivalent to bus.dispatch(...)
    assert child.event_status != 'completed'
    assert child_event_async.event_parent_id == event.event_id
    await child_event_async

    # or you can dispatch an event and block until it finishes processing by awaiting the event
    # this recursively waits for all handlers, including if event is forwarded to other buses
    # (note: awaiting an event from inside a handler jumps the FIFO queue and will process it immediately, before any other pending events)
    child_event_sync = await bus.dispatch(ChildEvent())
    # ChildEvent handlers run immediately
    assert child_event_sync.event_status == 'completed'

    # in all cases, parent-child relationships are automagically tracked
    assert child_event_sync.event_parent_id == event.event_id

async def run_main():
    bus.on(ChildEvent, child_handler)
    bus.on(ParentEvent, parent_handler)

    parent_event = bus.dispatch(ParentEvent())
    print(parent_event.event_children)           # show all the child events emitted during handling of an event
    await parent_event
    print(bus.log_tree())
    await bus.stop()

if __name__ == '__main__':
    asyncio.run(run_main())
```

<img width="1145" alt="image" src="https://github.com/user-attachments/assets/f94684a6-7694-4066-b948-46925f47b56c" />


### ⏳ Expect an Event to be Dispatched

Wait for specific events to be seen on a bus with optional filtering:

```python
# Block until a specific event is seen (with optional timeout)
request_event = await bus.dispatch(RequestEvent(...))
response_event = await bus.expect('ResponseEvent', timeout=30)

# Block until a specific event is seen (with optional predicate filtering)
response_event = await bus.expect(
    ResponseEvent,  # can pass event type as a string or class
    predicate=lambda e: e.request_id == my_request_id,
    timeout=30
)
```

> [!IMPORTANT]
> `expect()` resolves when the event is first *dispatched* to the `EventBus`, not when it completes. `await response_event` to get the completed event.

### 📝 Write-Ahead Logging

Persist events automatically for durability and debugging:

```python
# Enable WAL persistence
bus = EventBus(name='MyBus', wal_path='./events.jsonl')

# All completed events are automatically persisted
bus.dispatch(ImportantEvent(data="critical"))

# Events are saved as JSONL for easy processing
# {"event_type": "ImportantEvent", "data": "critical", ...}
```

<br/>

---
---

<br/>

## 📚 API Documentation

### `EventBus`

The main event bus class that manages event processing and handler execution.

```python
EventBus(
    name: str | None = None,
    wal_path: Path | str | None = None,
    parallel_handlers: bool = False,
    max_history_size: int | None = 50
)
```

**Parameters:**

- `name`: Optional unique name for the bus (auto-generated if not provided)
- `wal_path`: Path for write-ahead logging of events to a `jsonl` file (optional)
- `parallel_handlers`: If `True`, handlers run concurrently for each event, otherwise serially if `False` (the default)
- `max_history_size`: Maximum number of events to keep in history (default: 50, None = unlimited)

#### `EventBus` Properties

- `name`: The bus identifier
- `id`: Unique UUID7 for this bus instance
- `event_history`: Dict of all events the bus has seen by event_id (limited by `max_history_size`)
- `events_pending`: List of events waiting to be processed
- `events_started`: List of events currently being processed
- `events_completed`: List of completed events
- `all_instances`: Class-level WeakSet tracking all active EventBus instances (for memory monitoring)


#### `EventBus` Methods

##### `on(event_type: str | Type[BaseEvent], handler: Callable)`

Subscribe a handler to events matching a specific event type or `'*'` for all events.

```python
bus.on('UserEvent', handler_func)  # By event type string
bus.on(UserEvent, handler_func)    # By event class
bus.on('*', handler_func)          # Wildcard - all events
```

##### `dispatch(event: BaseEvent) -> BaseEvent`

Enqueue an event for processing and return the pending `Event` immediately (synchronous).

```python
event = bus.dispatch(MyEvent(data="test"))
result = await event  # await the pending Event to get the completed Event
```

**Note:** When `max_history_size` is set, EventBus enforces a hard limit of 100 pending events (queue + processing) to prevent runaway memory usage. Dispatch will raise `RuntimeError` if this limit is exceeded.

##### `expect(event_type: str | Type[BaseEvent], timeout: float | None=None, predicate: Callable[[BaseEvent], bool]=None) -> BaseEvent`

Wait for a specific event to occur.

```python
# Wait for any UserEvent
event = await bus.expect('UserEvent', timeout=30)

# Wait with custom filter
event = await bus.expect(
    'UserEvent',
    predicate=lambda e: e.user_id == 'specific_user'
)
```

##### `wait_until_idle(timeout: float | None=None)`

Wait until all events are processed and the bus is idle.

```python
await bus.wait_until_idle()             # wait indefinitely until EventBus has finished processing all events

await bus.wait_until_idle(timeout=5.0)  # wait up to 5 seconds
```

##### `stop(timeout: float | None=None, clear: bool=False)`

Stop the event bus, optionally waiting for pending events and clearing memory.

```python
await bus.stop(timeout=1.0)  # Graceful stop, wait up to 1sec for pending and active events to finish processing
await bus.stop()             # Immediate shutdown, aborts all pending and actively processing events
await bus.stop(clear=True)   # Stop and clear all event history and handlers to free memory
```

---

### `BaseEvent`

Base class for all events. Subclass `BaseEvent` to define your own events.

Make sure none of your own event data fields start with `event_` or `model_` to avoid clashing with `BaseEvent` or `pydantic` builtin attrs.

#### `BaseEvent` Fields

```python
class BaseEvent(BaseModel):
    # Framework-managed fields
    event_type: str              # Defaults to class name
    event_id: str                # Unique UUID7 identifier, auto-generated if not provided
    event_timeout: float = 60.0  # Maximum execution in seconds for each handler
    event_schema: str            # Module.Class@version (auto-set based on class & LIBRARY_VERSION env var)
    event_parent_id: str         # Parent event ID (auto-set)
    event_path: list[str]        # List of bus names traversed (auto-set)
    event_created_at: datetime   # When event was created, auto-generated
    event_results: dict[str, EventResult]   # Handler results
    
    # Data fields
    # ... subclass BaseEvent to add your own event data fields here ...
    # some_key: str
    # some_other_key: dict[str, int]
    # ...
```

`event.event_results` contains a dict of pending `EventResult` objects that will be completed once handlers finish executing.


#### `BaseEvent` Properties

- `event_status`: `Literal['pending', 'started', 'complete']` Event status
- `event_started_at`: `datetime` When first handler started processing
- `event_completed_at`: `datetime` When all handlers completed processing
- `event_children`: `list[BaseEvent]` Get any child events emitted during handling of this event
- `event_bus`: `EventBus` Shortcut to get the bus currently processing this event

#### `BaseEvent` Methods

##### `await event`

Await the `Event` object directly to get the completed `Event` object once all handlers have finished executing.

```python
event = bus.dispatch(MyEvent())
completed_event = await event

raw_result_values = [(await event_result) for event_result in completed_event.event_results.values()]
# equivalent to: completed_event.event_results_list()  (see below)
```

##### `event_result(timeout: float | None=None) -> Any`

Utility method helper to execute all the handlers and return the first handler's raw result value.

```python
result = await event.event_result()
```

##### `event_results_by_handler_id(timeout: float | None=None) -> dict`

Utility method helper to get all raw result values organized by `{handler_id: result_value}`.

```python
results = await event.event_results_by_handler_id()
# {'handler_id_1': result1, 'handler_id_2': result2}
```

##### `event_results_list(timeout: float | None=None) -> list[Any]`

Utility method helper to get all raw result values in a list.

```python
results = await event.event_results_list()
# [result1, result2]
```

##### `event_results_flat_dict(timeout: float | None=None) -> dict`

Utility method helper to merge all raw result values that are `dict`s into a single flat `dict`.

```python
results = await event.event_results_flat_dict()
# {'key1': 'value1', 'key2': 'value2'}
```

##### `event_results_flat_list(timeout: float | None=None) -> list`

Utility method helper to merge all raw result values that are `list`s into a single flat `list`.

```python
results = await event.event_results_flat_list()
# ['item1', 'item2', 'item3']
```

##### `event_bus` (property)

Shortcut to get the `EventBus` that is currently processing this event. Can be used to avoid having to pass an `EventBus` instance to your handlers.

```python
bus = EventBus()

async def some_handler(event: MyEvent):
    # You can always dispatch directly to any bus you have a reference to
    child_event = bus.dispatch(ChildEvent())
    
    # OR use the event.event_bus shortcut to get the current bus:
    child_event = await event.event_bus.dispatch(ChildEvent())
```


---

### `EventResult`

The placeholder object that represents the pending result from a single handler executing an event.  
`Event.event_results` contains a `dict[PythonIdStr, EventResult]` in the shape of `{handler_id: EventResult()}`.

You shouldn't need to ever directly use this class, it's an internal wrapper to track pending and completed results from each handler within `BaseEvent.event_results`.

#### `EventResult` Fields

```python
class EventResult(BaseModel):
    id: str                    # Unique identifier
    handler_id: str           # Handler function ID
    handler_name: str         # Handler function name
    eventbus_id: str          # Bus that executed this handler
    eventbus_name: str        # Bus name
    
    status: str               # 'pending', 'started', 'completed', 'error'
    result: Any               # Handler return value
    error: str | None         # Error message if failed
    
    started_at: datetime      # When handler started
    completed_at: datetime    # When handler completed
    timeout: float            # Handler timeout in seconds
    child_events: list[BaseEvent] # list of child events emitted during handler execution
```

#### `EventResult` Methods

##### `await result`

Await the `EventResult` object directly to get the raw result value.

```python
handler_result = event.event_results['handler_id']
value = await handler_result  # Returns result or raises an exception if handler hits an error
```

---

## 🧵 Advanced Concurrency Control

### `@retry` Decorator

The `@retry` decorator provides automatic retry functionality with built-in concurrency control for any function, including event handlers. This is particularly useful when handlers interact with external services that may temporarily fail.

```python
from bubus import EventBus, BaseEvent
from bubus.helpers import retry

bus = EventBus()

class FetchDataEvent(BaseEvent):
    url: str

@retry(
    wait=2,                     # Wait 2 seconds between retries
    retries=3,                  # Retry up to 3 times after initial failure
    timeout=5,                  # Each attempt times out after 5 seconds
    semaphore_limit=5,          # Max 5 concurrent executions
    backoff_factor=1.5,         # Exponential backoff: 2s, 3s, 4.5s
    retry_on=(TimeoutError, ConnectionError)  # Only retry on specific exceptions
)
async def fetch_with_retry(event: FetchDataEvent):
    # This handler will automatically retry on network failures
    async with aiohttp.ClientSession() as session:
        async with session.get(event.url) as response:
            return await response.json()

bus.on(FetchDataEvent, fetch_with_retry)
```

#### Retry Parameters

- **`timeout`**: Maximum amount of time function is allowed to take per attempt, in seconds (default: 5)
- **`retries`**: Number of additional retry attempts if function raises an exception (default: 3)
- **`retry_on`**: Tuple of exception types to retry on (default: `None` = retry on any `Exception`)
- **`wait`**: Base seconds to wait between retries (default: 3)
- **`backoff_factor`**: Multiplier for wait time after each retry (default: 1.0)
- **`semaphore_limit`**: Maximum number of concurrent calls that can run at the same time
- **`semaphore_scope`**: Scope for the semaphore: `class`, `self`, `global`, or `multiprocess`
- **`semaphore_timeout`**: Maximum time to wait for a semaphore slot before proceeding or failing
- **`semaphore_lax`**: Continue anyway if semaphore fails to be acquired in within the given time
- **`semaphore_name`**: Unique semaphore name to allow sharing a semaphore between functions

#### Semaphore Options

Control concurrency with built-in semaphore support:

```python
# Global semaphore - all calls share one limit
@retry(semaphore_limit=3, semaphore_scope='global')
async def global_limited_handler(event): ...

# Per-class semaphore - all instances of a class share one limit
class MyService:
    @retry(semaphore_limit=2, semaphore_scope='class')
    async def class_limited_handler(self, event): ...

# Per-instance semaphore - each instance gets its own limit
class MyService:
    @retry(semaphore_limit=1, semaphore_scope='self')
    async def instance_limited_handler(self, event): ...

# Cross-process semaphore - all processes share one limit
@retry(semaphore_limit=5, semaphore_scope='multiprocess')
async def process_limited_handler(event): ...
```

#### Advanced Example

```python
import logging

# Configure logging to see retry attempts
logging.basicConfig(level=logging.INFO)

class DatabaseEvent(BaseEvent):
    query: str

class DatabaseService:
    @retry(
        wait=1,
        retries=5,
        timeout=10,
        semaphore_limit=10,          # Max 10 concurrent DB operations
        semaphore_scope='class',     # Shared across all instances
        semaphore_timeout=30,        # Wait up to 30s for semaphore
        semaphore_lax=False,         # Fail if can't acquire semaphore
        backoff_factor=2.0,          # Exponential backoff: 1s, 2s, 4s, 8s, 16s
        retry_on=(ConnectionError, TimeoutError)
    )
    async def execute_query(self, event: DatabaseEvent):
        # Automatically retries on connection failures
        # Limited to 10 concurrent operations across all instances
        result = await self.db.execute(event.query)
        return result

# Register the handler
db_service = DatabaseService()
bus.on(DatabaseEvent, db_service.execute_query)
```

<br/>

---
---

<br/>

## 👾 Development

Set up the development environment using `uv`:

```bash
git clone https://github.com/browser-use/bubus && cd bubus

# Create virtual environment with Python 3.12
uv venv --python 3.12

# Activate virtual environment (varies by OS)
source .venv/bin/activate  # On Unix/macOS
# or
.venv\Scripts\activate  # On Windows

# Install dependencies
uv sync --dev --all-extras
```

```bash
# Run linter & type checker
uv run ruff check --fix
uv run ruff format
uv run pyright

# Run all tests
uv run pytest -vxs --full-trace tests/

# Run specific test file
uv run pytest tests/test_eventbus.py
```

## 🔗 Inspiration

- https://www.cosmicpython.com/book/chapter_08_events_and_message_bus.html#message_bus_diagram ⭐️
- https://developer.mozilla.org/en-US/docs/Web/API/EventTarget ⭐️
- https://github.com/pytest-dev/pluggy ⭐️
- https://github.com/teamhide/fastapi-event ⭐️
- https://github.com/ethereum/lahja ⭐️
- https://github.com/enricostara/eventure ⭐️
- https://github.com/akhundMurad/diator ⭐️
- https://github.com/n89nanda/pyeventbus
- https://github.com/iunary/aioemit
- https://github.com/dboslee/evently
- https://github.com/ArcletProject/Letoderea
- https://github.com/seanpar203/event-bus
- https://github.com/n89nanda/pyeventbus
- https://github.com/nicolaszein/py-async-bus
- https://github.com/AngusWG/simple-event-bus
- https://www.joeltok.com/posts/2021-03-building-an-event-bus-in-python/

## 🏛️ License

This project is licensed under the MIT License. For more information, see the main browser-use repository: https://github.com/browser-use/browser-use
