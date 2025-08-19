import asyncio
import contextvars
import inspect
import logging
import traceback
import warnings
import weakref
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Literal, TypeVar, cast, overload

import anyio  # pyright: ignore[reportMissingImports]
from uuid_extensions import uuid7str  # pyright: ignore[reportMissingImports, reportUnknownVariableType]

uuid7str: Callable[[], str] = uuid7str  # pyright: ignore

from bubus.models import (
    BUBUS_LOG_LEVEL,
    AsyncEventHandlerClassMethod,
    AsyncEventHandlerFunc,
    AsyncEventHandlerMethod,
    BaseEvent,
    ContravariantEventHandler,
    EventHandler,
    EventHandlerClassMethod,
    EventHandlerFunc,
    EventHandlerMethod,
    PythonIdentifierStr,
    PythonIdStr,
    T_Event,
    T_EventResultType,
    UUIDStr,
    get_handler_id,
    get_handler_name,
)

logger = logging.getLogger('bubus')
logger.setLevel(BUBUS_LOG_LEVEL)


# Define our own QueueShutDown exception
class QueueShutDown(Exception):
    """Raised when putting on to or getting from a shut-down Queue."""

    pass


QueueEntryType = TypeVar('QueueEntryType', bound='BaseEvent[Any]')
T_ExpectedEvent = TypeVar('T_ExpectedEvent', bound='BaseEvent[Any]')

EventPatternType = PythonIdentifierStr | Literal['*'] | type['BaseEvent[Any]']

# EventBusMiddleware will be imported dynamically to avoid circular imports


class CleanShutdownQueue(asyncio.Queue[QueueEntryType]):
    """asyncio.Queue subclass that handles shutdown cleanly without warnings."""

    _is_shutdown: bool = False
    _getters: deque[asyncio.Future[QueueEntryType]]
    _putters: deque[asyncio.Future[QueueEntryType]]

    def shutdown(self, immediate: bool = True):
        """Shutdown the queue and clean up all pending futures."""
        self._is_shutdown = True

        # Cancel all waiting getters without triggering warnings
        while self._getters:
            getter = self._getters.popleft()
            if not getter.done():
                # Set exception instead of cancelling to avoid "Event loop is closed" errors
                getter.set_exception(QueueShutDown())

        # Cancel all waiting putters
        while self._putters:
            putter = self._putters.popleft()
            if not putter.done():
                putter.set_exception(QueueShutDown())

    async def get(self) -> QueueEntryType:
        """Remove and return an item from the queue, with shutdown support."""
        while self.empty():
            if self._is_shutdown:
                raise QueueShutDown

            getter: asyncio.Future[QueueEntryType] = self._get_loop().create_future()  # type: ignore
            assert isinstance(getter, asyncio.Future)
            self._getters.append(getter)  # type: ignore[arg-type]
            try:
                await getter
            except:
                # Clean up the getter if we're cancelled
                getter.cancel()  # Just in case getter is not done yet.
                try:
                    self._getters.remove(getter)  # type: ignore[arg-type]
                except ValueError:
                    pass
                # Re-raise the exception
                raise

        return self.get_nowait()

    async def put(self, item: QueueEntryType) -> None:
        """Put an item into the queue, with shutdown support."""
        while self.full():
            if self._is_shutdown:
                raise QueueShutDown

            putter: asyncio.Future[QueueEntryType] = self._get_loop().create_future()  # type: ignore
            assert isinstance(putter, asyncio.Future)
            self._putters.append(putter)  # type: ignore[arg-type]
            try:
                await putter
            except:
                putter.cancel()  # Just in case putter is not done yet.
                try:
                    self._putters.remove(putter)  # type: ignore[arg-type]
                except ValueError:
                    pass
                raise

        return self.put_nowait(item)

    def put_nowait(self, item: QueueEntryType) -> None:
        """Put an item into the queue without blocking, with shutdown support."""
        if self._is_shutdown:
            raise QueueShutDown
        return super().put_nowait(item)

    def get_nowait(self) -> QueueEntryType:
        """Remove and return an item if one is immediately available, with shutdown support."""
        if self._is_shutdown and self.empty():
            raise QueueShutDown
        return super().get_nowait()


# Context variable to track the current event being processed (for setting event_parent_id from inside a child event)
_current_event_context: ContextVar['BaseEvent[Any] | None'] = ContextVar('current_event', default=None)
# Context variable to track if we're inside a handler (for nested event detection)
inside_handler_context: ContextVar[bool] = ContextVar('inside_handler', default=False)
# Context variable to track if we hold the global lock (for re-entrancy across tasks)
holds_global_lock: ContextVar[bool] = ContextVar('holds_global_lock', default=False)
# Context variable to track the current handler ID (for tracking child events)
_current_handler_id_context: ContextVar[str | None] = ContextVar('current_handler_id', default=None)


class ReentrantLock:
    """A re-entrant lock that works across different asyncio tasks using ContextVar."""

    def __init__(self):
        self._semaphore: asyncio.Semaphore | None = None
        self._depth = 0  # Track re-entrance depth
        self._loop: asyncio.AbstractEventLoop | None = None

    def _get_semaphore(self) -> asyncio.Semaphore:
        """Get or create the semaphore for the current event loop."""
        current_loop = asyncio.get_running_loop()
        if self._semaphore is None or self._loop != current_loop:
            # Create new semaphore for this event loop
            self._semaphore = asyncio.Semaphore(1)
            self._loop = current_loop
        return self._semaphore

    async def __aenter__(self):
        if holds_global_lock.get():
            # We already hold the lock in this context, increment depth
            self._depth += 1
            return self

        # Acquire the lock
        await self._get_semaphore().acquire()
        holds_global_lock.set(True)
        self._depth = 1
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None:
        if not holds_global_lock.get():
            # We don't hold the lock, nothing to do
            return

        self._depth -= 1
        if self._depth == 0:
            # Last exit, release the lock
            holds_global_lock.set(False)
            self._get_semaphore().release()

    def locked(self) -> bool:
        """Check if the lock is currently held."""
        # If semaphore doesn't exist yet or is from a different loop, it's not locked
        try:
            current_loop = asyncio.get_running_loop()
            if self._semaphore is None or self._loop != current_loop:
                return False
            return self._semaphore.locked()
        except RuntimeError:
            # No running loop, can't check
            return False


# Global re-entrant lock shared by all EventBus instances
_global_eventbus_lock: ReentrantLock | None = None


def _get_global_lock() -> ReentrantLock:
    """Get or create the global EventBus lock."""
    global _global_eventbus_lock
    if _global_eventbus_lock is None:
        _global_eventbus_lock = ReentrantLock()
    return _global_eventbus_lock


def _log_pretty_path(path: Path | str | None) -> str:
    """Pretty-print a path, shorten home dir to ~ and cwd to ."""

    if not path or not str(path).strip():
        return ''  # always falsy in -> falsy out so it can be used in ternaries

    # dont print anything thats not a path
    if not isinstance(path, (str, Path)):  # type: ignore
        # no other types are safe to just str(path) and log to terminal unless we know what they are
        # e.g. what if we get storage_date=dict | Path and the dict version could contain real cookies
        return f'<{type(path).__name__}>'

    # replace home dir and cwd with ~ and .
    pretty_path = str(path).replace(str(Path.home()), '~').replace(str(Path.cwd().resolve()), '.')

    # wrap in quotes if it contains spaces
    if pretty_path.strip() and ' ' in pretty_path:
        pretty_path = f'"{pretty_path}"'

    return pretty_path

def _log_filtered_traceback(exc: BaseException) -> str:
    te = traceback.TracebackException.from_exception(exc, capture_locals=False)

    def _filter(te: traceback.TracebackException):
        te.stack = traceback.StackSummary.from_list([
            f for f in te.stack
            if "asyncio/tasks.py" not in f.filename and "lib/python" not in f.filename
        ])
        if te.__cause__:
            _filter(te.__cause__)
        if te.__context__:
            _filter(te.__context__)

    _filter(te)
    return "".join(te.format())


class EventBus:
    """
    Async event bus with write-ahead logging and guaranteed FIFO processing.

    Features:
    - Enqueue events synchronously, await their results using 'await Event()'
    - FIFP Write-ahead logging with UUIDs and timestamps,
    - Serial event processing, parallel handler execution per event
    """

    # Track all EventBus instances (using weakrefs to allow garbage collection)
    all_instances: weakref.WeakSet['EventBus'] = weakref.WeakSet()

    # Class Attributes
    name: PythonIdentifierStr = 'EventBus'
    parallel_handlers: bool = False
    wal_path: Path | None = None

    # Runtime State
    id: UUIDStr = '00000000-0000-0000-0000-000000000000'
    handlers: dict[PythonIdStr, list[ContravariantEventHandler['BaseEvent[Any]']]]  # collected by .on(<event_type>, <handler>)
    event_queue: CleanShutdownQueue['BaseEvent[Any]'] | None
    event_history: dict[UUIDStr, 'BaseEvent[Any]']  # collected by .dispatch(<event>)

    _is_running: bool = False
    _runloop_task: asyncio.Task[None] | None = None
    _on_idle: asyncio.Event | None = None

    def __init__(
        self,
        name: PythonIdentifierStr | None = None,
        wal_path: Path | str | None = None,
        parallel_handlers: bool = False,
        max_history_size: int | None = 50,  # Keep only 50 events in history
        middlewares: list[Any] | None = None,
    ):
        self.id = uuid7str()
        self.name = name or f'{self.__class__.__name__}_{self.id[-8:]}'
        assert self.name.isidentifier(), f'EventBus name must be a unique identifier string, got: {self.name}'

        # Force garbage collection to clean up any dead EventBus instances in the WeakSet
        # gc.collect()  # Commented out - this is expensive and causes 5s delays when creating many EventBus instances

        # Check for name uniqueness among existing instances
        # We'll collect potential conflicts and check if they're still alive
        original_name = self.name
        conflicting_buses: list[EventBus] = []

        for existing_bus in list(EventBus.all_instances):  # Make a list copy to avoid modification during iteration
            if existing_bus is not self and existing_bus.name == self.name:
                # Try to trigger collection of just this object by checking if it's collectable
                # First, temporarily remove from WeakSet to see if that was the only reference
                EventBus.all_instances.discard(existing_bus)

                # Check if the object is still reachable by creating a new weak reference
                # If the object only existed in the WeakSet, it should be unreachable now
                try:
                    # Try to access an attribute to see if the object is still valid
                    _ = existing_bus.name  # This will work if object is still alive

                    # Object is still alive with real references, restore to WeakSet
                    EventBus.all_instances.add(existing_bus)
                    conflicting_buses.append(existing_bus)
                except Exception:
                    # Object was garbage collected or is invalid (e.g., AttributeError), that's fine
                    # Don't re-add to WeakSet, let it stay removed
                    pass

        # If we found conflicting buses, auto-generate a unique suffix
        if conflicting_buses:
            # Generate a unique suffix using the last 8 chars of a UUID
            unique_suffix = uuid7str()[-8:]
            self.name = f'{original_name}_{unique_suffix}'

            warnings.warn(
                f'⚠️ EventBus with name "{original_name}" already exists. '
                f'Auto-generated unique name: "{self.name}" to avoid conflicts. '
                f'Consider using unique names or stop(clear=True) on unused buses.',
                UserWarning,
                stacklevel=2,
            )

        self.event_queue = None
        self.event_history = {}
        self.handlers = defaultdict(list)
        self.parallel_handlers = parallel_handlers
        self.wal_path = Path(wal_path) if wal_path else None
        self._on_idle = None
        
        # Set up middlewares, adding WAL middleware if wal_path is provided
        self.middlewares = middlewares or []
        if wal_path:
            # Import here to avoid circular imports
            from bubus.middleware import WALEventBusMiddleware
            self.middlewares.append(WALEventBusMiddleware(wal_path))

        # Memory leak prevention settings
        self.max_history_size = max_history_size

        # Register this instance
        EventBus.all_instances.add(self)

        # Instead of registering as normal event handlers,
        # these special handlers are just called manually at the end of _run_loop_step
        # self.on('*', self._default_log_handler)
        # self.on('*', self._default_wal_handler)

    def __del__(self):
        """Auto-cleanup on garbage collection"""
        # Most cleanup should have been done by the event loop close hook
        # This is just a fallback for any remaining cleanup

        # Signal the run loop to stop
        self._is_running = False

        # Our custom queue handles cleanup properly in shutdown()
        # No need for manual cleanup here

        # Check total memory usage across all EventBus instances
        try:
            self._check_total_memory_usage()
        except Exception:
            # Don't let memory check errors prevent cleanup
            pass

    def __str__(self) -> str:
        icon = '🟢' if self._is_running else '🔴'
        return f'{self.name}{icon}(⏳ {len(self.events_pending or [])} | ▶️ {len(self.events_started or [])} | ✅ {len(self.events_completed or [])} ➡️ {len(self.handlers)} 👂)'

    def __repr__(self) -> str:
        return str(self)

    @property
    def events_pending(self) -> list['BaseEvent[Any]']:
        """Get events that haven't started processing yet (does not include events that have not even finished dispatching yet in self.event_queue)"""
        return [
            event for event in self.event_history.values() if event.event_started_at is None and event.event_completed_at is None
        ]

    @property
    def events_started(self) -> list['BaseEvent[Any]']:
        """Get events currently being processed"""
        return [event for event in self.event_history.values() if event.event_started_at and not event.event_completed_at]

    @property
    def events_completed(self) -> list['BaseEvent[Any]']:
        """Get events that have completed processing"""
        return [event for event in self.event_history.values() if event.event_completed_at is not None]

    # Overloads for typed event patterns with specific handler signatures
    # Order matters - more specific types must come before general ones

    # 1. EventHandlerFunc[T_Event] - sync function taking event
    @overload
    def on(self, event_pattern: EventPatternType, handler: EventHandlerFunc[T_Event]) -> None: ...

    # 2. AsyncEventHandlerFunc[T_Event] - async function taking event
    @overload
    def on(self, event_pattern: EventPatternType, handler: AsyncEventHandlerFunc[T_Event]) -> None: ...

    # 3. EventHandlerMethod[T_Event] - sync method taking self and event
    @overload
    def on(self, event_pattern: EventPatternType, handler: EventHandlerMethod[T_Event]) -> None: ...

    # 4. AsyncEventHandlerMethod[T_Event] - async method taking self and event
    @overload
    def on(self, event_pattern: EventPatternType, handler: AsyncEventHandlerMethod[T_Event]) -> None: ...

    # 5. EventHandlerClassMethod[BaseEvent] - sync classmethod taking cls and event
    @overload
    def on(self, event_pattern: EventPatternType, handler: EventHandlerClassMethod['BaseEvent[Any]']) -> None: ...

    # 6. AsyncEventHandlerClassMethod[BaseEvent] - async classmethod taking cls and event
    @overload
    def on(self, event_pattern: EventPatternType, handler: AsyncEventHandlerClassMethod['BaseEvent[Any]']) -> None: ...

    # I dont think this is needed, but leaving it here for now
    # 9. Coroutine[Any, Any, Any] - direct coroutine
    # @overload # type: ignore[reportUnknownReturnType]
    # def on(self, event_pattern: EventPatternType, handler: Coroutine[Any, Any, Any]) -> None: ...

    def on(
        self,
        event_pattern: EventPatternType,
        handler: (  # TypeAlias with args doesnt work on overloaded signature, has to be defined inline
            EventHandlerFunc[T_Event]
            | AsyncEventHandlerFunc['BaseEvent[Any]']
            | EventHandlerMethod[T_Event]
            | AsyncEventHandlerMethod['BaseEvent[Any]']
            | EventHandlerClassMethod['BaseEvent[Any]']
            | AsyncEventHandlerClassMethod['BaseEvent[Any]']
        ),
    ) -> None:
        """
        Subscribe to events matching a pattern, event type name, or event model class.
        Use event_pattern='*' to subscribe to all events. Handler can be sync or async function or method.

        Examples:
                eventbus.on('TaskStartedEvent', handler)  # Specific event type
                eventbus.on(TaskStartedEvent, handler)  # Event model class
                eventbus.on('*', handler)  # Subscribe to all events
                eventbus.on('*', other_eventbus.dispatch)  # Forward all events to another EventBus

        Note: When forwarding events between buses, all handler results are automatically
        flattened into the original event's results, so EventResults sees all handlers
        from all buses as a single flat collection.
        """
        assert isinstance(event_pattern, str) or issubclass(event_pattern, BaseEvent), (
            f'Invalid event pattern: {event_pattern}, must be a string event type or subclass of BaseEvent'
        )
        assert inspect.isfunction(handler) or inspect.ismethod(handler) or inspect.iscoroutinefunction(handler), (
            f'Invalid handler: {handler}, must be a sync or async function or method'
        )

        # Determine event key
        event_key: str
        if event_pattern == '*':
            event_key = '*'
        elif isinstance(event_pattern, type) and issubclass(event_pattern, BaseEvent):  # pyright: ignore[reportUnnecessaryIsInstance]
            event_key = event_pattern.__name__  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        else:
            event_key = str(event_pattern)

        # Ensure event_key is definitely a string at this point
        assert isinstance(event_key, str)

        # Check for duplicate handler names
        new_handler_name = get_handler_name(handler)
        existing_registered_handlers = [get_handler_name(h) for h in self.handlers.get(event_key, [])]  # pyright: ignore[reportUnknownArgumentType]

        if new_handler_name in existing_registered_handlers:
            warnings.warn(
                f"⚠️ {self} Handler {new_handler_name} already registered for event '{event_key}'. "
                f'This may cause ambiguous results when using name-based access. '
                f'Consider using unique function names.',
                UserWarning,
                stacklevel=2,
            )

        # Register handler
        self.handlers[event_key].append(handler)  # type: ignore
        logger.debug(f'👂 {self}.on({event_key}, {get_handler_name(handler)}) Registered event handler')

    def dispatch(self, event: 'BaseEvent[Any]') -> 'BaseEvent[Any]':
        """
        Enqueue an event for processing and immediately return an Event(status='pending') version (synchronous).
        You can then await the Event(status='pending') object to block until its Event(status='completed') versionis ready,
        or you can interact with the unawaited Event(status='pending') before its handlers have finished.

        (The first EventBus.dispatch() call will auto-start a bus's async _run_loop() if it's not already running)

        >>> completed_event = await eventbus.dispatch(SomeEvent())
                # 1. enqueues the event synchronously
                # 2. returns an awaitable SomeEvent() with pending results in .event_results
                # 3. awaits the SomeEvent() which waits until all pending results are complete and returns the completed SomeEvent()

        >>> result_value = await eventbus.dispatch(SomeEvent()).event_result()
                # 1. enqueues the event synchronously
                # 2. returns a pending SomeEvent() with pending results in .event_results
                # 3. awaiting .event_result() waits until all pending results are complete, and returns the raw result value of the first one
        """

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError(f'{self}.dispatch() called but no event loop is running! Event not queued: {event.event_type}')

        assert event.event_id, 'Missing event.event_id: UUIDStr = uuid7str()'
        assert event.event_created_at, 'Missing event.event_created_at: datetime = datetime.now(UTC)'
        assert event.event_type and event.event_type.isidentifier(), 'Missing event.event_type: str'
        assert event.event_schema and '@' in event.event_schema, 'Missing event.event_schema: str (with @version)'

        # Automatically set event_parent_id from context if not already set
        if event.event_parent_id is None:
            current_event: 'BaseEvent[Any] | None' = _current_event_context.get()
            if current_event is not None:
                event.event_parent_id = current_event.event_id

        # Track child events - if we're inside a handler, add this event to the handler's event_children list
        # Only track if this is a NEW event (not forwarding an existing event)
        current_handler_id = _current_handler_id_context.get()
        if current_handler_id is not None and inside_handler_context.get():
            current_event = _current_event_context.get()
            if current_event is not None and current_handler_id in current_event.event_results:
                # Only add as child if it's a different event (not forwarding the same event)
                if event.event_id != current_event.event_id:
                    current_event.event_results[current_handler_id].event_children.append(event)

        # Add this EventBus to the event_path if not already there
        if self.name not in event.event_path:
            # preserve identity of the original object instead of creating a new one, so that the original object remains awaitable to get the result
            # NOT: event = event.model_copy(update={'event_path': event.event_path + [self.name]})
            event.event_path.append(self.name)
        else:
            logger.debug(
                f'⚠️ {self}.dispatch({event.event_type}) - Bus already in path, not adding again. Path: {event.event_path}'
            )

        assert event.event_path, 'Missing event.event_path: list[str] (with at least the origin function name recorded in it)'
        assert all(entry.isidentifier() for entry in event.event_path), (
            f'Event.event_path must be a list of valid EventBus names, got: {event.event_path}'
        )

        # Check hard limit on total pending events (queue + in-progress)
        # Only enforce if we have memory limits set
        if self.max_history_size is not None:
            queue_size = self.event_queue.qsize() if self.event_queue else 0
            pending_in_history = sum(1 for e in self.event_history.values() if e.event_status in ('pending', 'started'))
            total_pending = queue_size + pending_in_history

            if total_pending >= 100:
                raise RuntimeError(
                    f'EventBus at capacity: {total_pending} pending events (100 max). '
                    f'Queue: {queue_size}, Processing: {pending_in_history}. '
                    f'Cannot accept new events until some complete.'
                )

        # Auto-start if needed
        self._start()

        # Put event in queue synchronously using put_nowait
        if self.event_queue:
            try:
                self.event_queue.put_nowait(event)
                # Only add to history after successfully queuing
                self.event_history[event.event_id] = event
                logger.info(
                    f'🗣️ {self}.dispatch({event.event_type}) ➡️ Event#{event.event_id[-8:]}({event.event_status} #{self.event_queue.qsize()})'
                )
            except asyncio.QueueFull:
                # Don't add to history if we can't queue it
                logger.error(
                    f'⚠️ {self} Event queue is full! Dropping event and aborting {event.event_type}:\n{event.model_dump_json()}'  # pyright: ignore[reportUnknownMemberType]
                )
                raise  # could also block indefinitely until queue has space, but dont drop silently or delete events
        else:
            logger.warning(f'⚠️ {self}.dispatch() called but event_queue is None! Event not queued: {event.event_type}')

        # Note: We do NOT pre-create EventResults here anymore.
        # EventResults are created only when handlers actually start executing.
        # This avoids "orphaned" pending results for handlers that get filtered out later.

        # Clean up if over the limit
        if self.max_history_size and len(self.event_history) > self.max_history_size:
            self.cleanup_event_history()

        return event

    @overload
    async def expect(
        self,
        event_type: type[T_ExpectedEvent],
        include: Callable[['BaseEvent[Any]' | T_ExpectedEvent], bool] = lambda _: True,
        exclude: Callable[['BaseEvent[Any]' | T_ExpectedEvent], bool] = lambda _: False,
        predicate: Callable[['BaseEvent[Any]' | T_ExpectedEvent], bool] = lambda _: True,  # deprecated, alias for include
        timeout: float | None = None,
    ) -> T_ExpectedEvent: ...

    @overload
    async def expect(
        self,
        event_type: PythonIdentifierStr,
        include: Callable[['BaseEvent[Any]'], bool] = lambda _: True,
        exclude: Callable[['BaseEvent[Any]'], bool] = lambda _: False,
        predicate: Callable[['BaseEvent[Any]'], bool] = lambda _: True,  # deprecated, alias for include
        timeout: float | None = None,
    ) -> 'BaseEvent[Any]': ...

    async def expect(
        self,
        event_type: PythonIdentifierStr | type[T_ExpectedEvent],
        include: Callable[['BaseEvent[Any]'], bool] = lambda _: True,
        exclude: Callable[['BaseEvent[Any]'], bool] = lambda _: False,
        predicate: Callable[['BaseEvent[Any]'], bool] = lambda _: True,  # deprecated, alias for include
        timeout: float | None = None,
    ) -> 'BaseEvent[Any]' | T_ExpectedEvent:
        """
        Wait for an event matching the given type/pattern with optional filters.

        Args:
                event_type: The event type string or model class to wait for
                include: Filter function that must return True for the event to match (default: lambda e: True)
                exclude: Filter function that must return False for the event to match (default: lambda e: False)
                predicate: Deprecated name, alias for include (default: lambda e: True)
                timeout: Maximum time to wait in seconds as a float (None = wait forever)

        Returns:
                The first matching event

        Raises:
                asyncio.TimeoutError: If timeout is reached before a matching event

        Example:
                # Wait for any response event
                response = await eventbus.expect('ResponseEvent', timeout=30)

                # Wait for specific response with include filter
                response = await eventbus.expect(
                        'ResponseEvent',
                        include=lambda e: e.request_id == my_request_id,
                        timeout=30
                )

                # Wait for response excluding certain types
                response = await eventbus.expect(
                        'ResponseEvent',
                        exclude=lambda e: e.error_code is not None,
                        timeout=30
                )
        """
        future: asyncio.Future['BaseEvent[Any]'] = asyncio.Future()

        # Handle backwards compatibility: merge predicate into include
        if predicate is not None:  # type: ignore[conditionAlwaysTrue]
            original_include = include
            include = lambda e, orig=original_include, pred=predicate: orig(e) and pred(e)

        def notify_expect_handler(event: 'BaseEvent[Any]') -> None:
            """Handler that resolves the future when a matching event is found"""
            if not future.done() and include(event) and not exclude(event):
                future.set_result(event)

        # make debugging otherwise ephemeral async expect handlers easier by including some metadata in the stacktrace func names
        current_frame = inspect.currentframe()
        assert current_frame
        notify_expect_handler.__name__ = f'{self}.expect({event_type}, timeout={timeout})@{_log_pretty_path(current_frame.f_code.co_filename)}:{current_frame.f_lineno}'  # add file and line number to the name

        # Register temporary listener that watches for matching events and triggers the expect handler
        self.on(event_type, notify_expect_handler)

        try:
            # Wait for the future with optional timeout
            if timeout is not None:
                return await asyncio.wait_for(future, timeout=timeout)
            else:
                return await future
        finally:
            # Clean up handler
            event_key: str = event_type.__name__ if isinstance(event_type, type) else str(event_type)  # pyright: ignore[reportUnknownMemberType, reportPartialTypeErrors]
            if event_key in self.handlers and notify_expect_handler in self.handlers[event_key]:
                self.handlers[event_key].remove(notify_expect_handler)

    def _start(self) -> None:
        """Start the event bus if not already running"""
        if not self._is_running:
            try:
                loop = asyncio.get_running_loop()

                # Hook into the event loop's close method to cleanup before it closes
                # this is necessary to silence "RuntimeError: no running event loop" and "event loop is closed" errors on shutdown
                if not hasattr(loop, '_eventbus_close_hooked'):
                    original_close = loop.close
                    registered_eventbuses: weakref.WeakSet[EventBus] = weakref.WeakSet()

                    def close_with_cleanup() -> None:
                        # Clean up all registered EventBuses before closing the loop
                        for eventbus in list(registered_eventbuses):
                            try:
                                # Stop the eventbus while loop is still running
                                if eventbus._is_running:
                                    eventbus._is_running = False

                                    # Shutdown the queue properly - our custom queue will handle cleanup
                                    if eventbus.event_queue:
                                        eventbus.event_queue.shutdown(immediate=True)

                                    if eventbus._runloop_task and not eventbus._runloop_task.done():
                                        # Suppress warning before cancelling
                                        if hasattr(eventbus._runloop_task, '_log_destroy_pending'):
                                            eventbus._runloop_task._log_destroy_pending = False  # type: ignore
                                        eventbus._runloop_task.cancel()
                            except Exception:
                                pass

                        # Now close the loop
                        original_close()

                    loop.close = close_with_cleanup
                    loop._eventbus_close_hooked = True  # type: ignore
                    loop._eventbus_instances = registered_eventbuses  # type: ignore

                # Register this EventBus instance in the WeakSet of all EventBuses on the loop
                if hasattr(loop, '_eventbus_instances'):
                    loop._eventbus_instances.add(self)  # type: ignore

                # Create async objects if needed
                if self.event_queue is None:
                    # Set queue size based on whether we have limits
                    queue_size = 50 if self.max_history_size is not None else 0  # 0 = unlimited
                    self.event_queue = CleanShutdownQueue['BaseEvent[Any]'](maxsize=queue_size)
                    self._on_idle = asyncio.Event()
                    self._on_idle.clear()  # Start in a busy state unless we confirm queue is empty by running _run_loop_step() at least once

                # Create and start the run loop task
                self._runloop_task = loop.create_task(self._run_loop(), name=f'{self}._run_loop')
                self._is_running = True
            except RuntimeError:
                # No event loop - will start when one becomes available
                pass

    async def stop(self, timeout: float | None = None, clear: bool = False) -> None:
        """Stop the event bus, optionally waiting for events to complete

        Args:
            timeout: Maximum time to wait for pending events to complete
            clear: If True, clear event history and remove from global tracking to free memory
        """
        if not self._is_running:
            return

        # Wait for completion if timeout specified and > 0
        # timeout=0 means "don't wait", so skip the wait entirely
        if timeout is not None and timeout > 0:
            try:
                await self.wait_until_idle(timeout=timeout)
            except TimeoutError:
                pass

        queue_size = self.event_queue.qsize() if self.event_queue else 0
        if queue_size or self.events_pending or self.events_started:
            logger.debug(
                f'⚠️ {self} stopping with pending events: Pending {len(self.events_pending) + queue_size} | Started {len(self.events_started)} | Completed {len(self.events_completed)}\n'
                f'PENDING={self.events_pending}\nSTARTED={self.events_started}'
            )

        # Signal shutdown
        self._is_running = False

        # Shutdown the queue to unblock any pending get() operations
        if self.event_queue:
            self.event_queue.shutdown()

        # Wait for the run loop task to finish / force-cancel it if it's hanging
        if self._runloop_task and not self._runloop_task.done():
            await asyncio.wait({self._runloop_task}, timeout=0.1)
            try:
                self._runloop_task.cancel()
            except Exception:
                pass

        # Clear references
        self._runloop_task = None
        if self._on_idle:
            self._on_idle.set()

        # Clear event history and handlers if requested (for memory cleanup)
        if clear:
            self.event_history.clear()
            self.handlers.clear()
            # Remove from global instance tracking
            if self in EventBus.all_instances:
                EventBus.all_instances.discard(self)

            # Remove from event loop's tracking if present
            try:
                loop = asyncio.get_running_loop()
                if hasattr(loop, '_eventbus_instances'):
                    loop._eventbus_instances.discard(self)  # type: ignore
            except RuntimeError:
                # No running loop, that's fine
                pass

            logger.debug(f'🧹 {self} cleared event history and removed from global tracking')

        logger.debug(f'🛑 {self} shut down gracefully' if timeout is not None else f'🛑 {self} killed')

        # Check total memory usage across all instances
        try:
            self._check_total_memory_usage()
        except Exception:
            # Don't let memory check errors prevent shutdown
            pass

    async def wait_until_idle(self, timeout: float | None = None) -> None:
        """Wait until the event bus is idle (no events being processed and all handlers completed)"""

        self._start()
        assert self._on_idle and self.event_queue, 'EventBus._start() must be called before wait_until_idle() is reached'

        start_time = asyncio.get_event_loop().time()
        remaining_timeout = timeout

        try:
            # First wait for the queue to be empty
            join_task = asyncio.create_task(self.event_queue.join())
            await asyncio.wait_for(join_task, timeout=remaining_timeout)

            # Update remaining timeout
            if timeout is not None:
                elapsed = asyncio.get_event_loop().time() - start_time
                remaining_timeout = max(0, timeout - elapsed)

            # Wait for idle state
            idle_task = asyncio.create_task(self._on_idle.wait())
            await asyncio.wait_for(idle_task, timeout=remaining_timeout)

            # Critical: Ensure the runloop has settled by yielding control
            # This allows the runloop to complete any in-flight operations
            # and prevents race conditions with event_history access
            await asyncio.sleep(0)  # Yield to event loop

            # Double-check we're truly idle - if new events came in, wait again
            while not self._on_idle.is_set() or self.events_started or self.events_pending:
                if timeout is not None:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    remaining_timeout = max(0, timeout - elapsed)
                    if remaining_timeout <= 0:
                        raise TimeoutError()

                # Clear and wait again
                self._on_idle.clear()
                idle_task = asyncio.create_task(self._on_idle.wait())
                await asyncio.wait_for(idle_task, timeout=remaining_timeout)
                await asyncio.sleep(0)  # Yield again

        except TimeoutError:
            logger.warning(
                f'⌛️ {self} Timeout waiting for event bus to be idle after {timeout}s (processing: {len(self.events_started)})'
            )

    async def _run_loop(self) -> None:
        """Main event processing loop"""
        try:
            while self._is_running:
                try:
                    _processed_event = await self._run_loop_step()
                    # Check if we should set idle state after processing
                    if self._on_idle and self.event_queue:
                        if not (self.events_pending or self.events_started or self.event_queue.qsize()):
                            self._on_idle.set()
                except QueueShutDown:
                    # Queue was shut down, exit cleanly
                    break
                except RuntimeError as e:
                    # Event loop is closing
                    if 'Event loop is closed' in str(e) or 'no running event loop' in str(e):
                        break
                    else:
                        logger.exception(f'❌ {self} Runtime error in event loop: {type(e).__name__} {e}', exc_info=True)
                        # Continue running even if there's an error
                except Exception as e:
                    logger.exception(f'❌ {self} Error in event loop: {type(e).__name__} {e}', exc_info=True)
                    # Continue running even if there's an error
        except asyncio.CancelledError:
            # Task was cancelled, clean exit
            # logger.debug(f'🛑 {self} Event loop task cancelled')
            pass
        finally:
            # Don't call stop() here as it might create new tasks
            self._is_running = False

    async def _get_next_event(self, wait_for_timeout: float = 0.1) -> 'BaseEvent[Any] | None':
        """Get the next event from the queue"""

        assert self._on_idle and self.event_queue, 'EventBus._start() must be called before _get_next_event()'
        if not self._is_running:
            return None

        try:
            # Create a task for queue.get() so we can cancel it cleanly
            get_next_queued_event = asyncio.create_task(self.event_queue.get())
            if hasattr(get_next_queued_event, '_log_destroy_pending'):
                get_next_queued_event._log_destroy_pending = False  # type: ignore  # Suppress warnings on this task in case of cleanup

            # Wait for next event with timeout
            has_next_event, _pending = await asyncio.wait({get_next_queued_event}, timeout=wait_for_timeout)
            if has_next_event:
                # Check if we're still running before returning the event
                if not self._is_running:
                    get_next_queued_event.cancel()
                    return None
                return await get_next_queued_event  # await to actually resolve it to the next event
            else:
                # Get task timed out, cancel it cleanly to suppress warnings
                get_next_queued_event.cancel()

                # Check if we're idle, if so, set the idle flag
                if not (self.events_pending or self.events_started or self.event_queue.qsize()):
                    self._on_idle.set()
                return None

        except (asyncio.CancelledError, RuntimeError, QueueShutDown):
            # Clean cancellation during shutdown or queue was shut down
            return None

    async def _run_loop_step(
        self, event: 'BaseEvent[Any] | None' = None, timeout: float | None = None, wait_for_timeout: float = 0.1
    ) -> 'BaseEvent[Any] | None':
        """Process a single event from the queue"""
        assert self._on_idle and self.event_queue, 'EventBus._start() must be called before _run_loop_step()'

        # Track if we got the event from the queue
        from_queue = False

        # Wait for next event with timeout to periodically check idle state
        if event is None:
            event = await self._get_next_event(wait_for_timeout=wait_for_timeout)
            from_queue = True
        if event is None:
            return None

        logger.debug(f'🏃 {self}._run_loop_step({event}) STARTING')

        # Clear idle state when we get an event
        self._on_idle.clear()

        # Always acquire the global lock (it's re-entrant across tasks)
        async with _get_global_lock():
            # Process the event
            await self.process_event(event, timeout=timeout)

            # Mark task as done only if we got it from the queue
            if from_queue:
                self.event_queue.task_done()

        logger.debug(f'✅ {self}._run_loop_step({event}) COMPLETE')
        return event

    async def process_event(self, event: 'BaseEvent[Any]', timeout: float | None = None) -> None:
        """Process a single event (assumes lock is already held)"""
        # Get applicable handlers
        applicable_handlers = self._get_applicable_handlers(event)
        
        # Create pending EventResults for all applicable handlers before execution
        # This ensures the event knows it has handlers and won't mark itself complete prematurely
        for handler_id, handler in applicable_handlers.items():
            if handler_id not in event.event_results:
                event.event_result_update(
                    handler=handler,
                    eventbus=self,
                    status='pending',
                    timeout=timeout or event.event_timeout
                )

        # Execute handlers
        await self._execute_handlers(event, handlers=applicable_handlers, timeout=timeout)

        await self._default_log_handler(event)

        # Mark event as complete if all handlers are done
        event.event_mark_complete_if_all_handlers_completed()

        # After processing this event, check if any parent events can now be marked complete
        # We do this by walking up the parent chain
        current = event
        checked_ids: set[str] = set()

        while current.event_parent_id and current.event_parent_id not in checked_ids:
            checked_ids.add(current.event_parent_id)

            # Find parent event in any bus's history
            parent_event = None
            # Create a list copy to avoid "Set changed size during iteration" error
            for bus in list(EventBus.all_instances):
                if bus and current.event_parent_id in bus.event_history:
                    parent_event = bus.event_history[current.event_parent_id]
                    break

            if not parent_event:
                break

            # Check if parent can be marked complete
            if parent_event.event_completed_signal and not parent_event.event_completed_signal.is_set():
                parent_event.event_mark_complete_if_all_handlers_completed()

            # Move up the chain
            current = parent_event

        # Clean up excess events to prevent memory leaks
        if self.max_history_size:
            self.cleanup_event_history()

    def _get_applicable_handlers(self, event: 'BaseEvent[Any]') -> dict[str, EventHandler]:
        """Get all handlers that should process the given event, filtering out those that would create loops"""
        applicable_handlers: list[EventHandler] = []

        # Add event-type-specific handlers
        applicable_handlers.extend(self.handlers.get(event.event_type, []))

        # Add wildcard handlers (handlers registered for '*')
        applicable_handlers.extend(self.handlers.get('*', []))

        # Filter out handlers that would create loops and build id->handler mapping
        # Use handler id as key to preserve all handlers even with duplicate names
        filtered_handlers: dict[PythonIdStr, EventHandler] = {}
        for handler in applicable_handlers:
            if self._would_create_loop(event, handler):
                continue
            else:
                handler_id = get_handler_id(handler, self)
                filtered_handlers[handler_id] = handler
                logger.debug(f'  Added handler {get_handler_name(handler)} with ID {handler_id}')

        return filtered_handlers

    async def _execute_handlers(
        self, event: 'BaseEvent[Any]', handlers: dict[PythonIdStr, EventHandler] | None = None, timeout: float | None = None
    ) -> None:
        """Execute all handlers for an event in parallel"""
        applicable_handlers = handlers if (handlers is not None) else self._get_applicable_handlers(event)
        if not applicable_handlers:
            event.event_mark_complete_if_all_handlers_completed()  # mark event completed immediately if it has no handlers
            return

        # Execute all handlers in parallel
        if self.parallel_handlers:
            handler_tasks: dict[PythonIdStr, tuple[asyncio.Task[Any], EventHandler]] = {}
            # Copy the current context to ensure context vars are propagated
            context = contextvars.copy_context()
            for handler_id, handler in applicable_handlers.items():
                task = asyncio.create_task(
                    self._execute_handler_with_middlewares(event, handler, timeout=timeout),
                    name=f'{self}._execute_handler_with_middlewares({event}, {get_handler_name(handler)})',
                    context=context,
                )
                handler_tasks[handler_id] = (task, handler)

            # Wait for all handlers to complete
            for handler_id, (task, handler) in handler_tasks.items():
                try:
                    await task
                except Exception:
                    # Error already logged and recorded in _execute_handler_with_middlewares
                    pass
        else:
            # otherwise, execute handlers serially, wait until each one completes before moving on to the next
            for handler_id, handler in applicable_handlers.items():
                try:
                    await self._execute_handler_with_middlewares(event, handler, timeout=timeout)
                except Exception as e:
                    # Error already logged and recorded in _execute_handler_with_middlewares
                    logger.debug(
                        f'❌ {self} Handler {get_handler_name(handler)}#{str(id(handler))[-4:]}({event}) failed with {type(e).__name__}: {e}'
                    )
                    pass

    async def _execute_handler_with_middlewares(
        self, event: 'BaseEvent[T_EventResultType]', handler: EventHandler, timeout: float | None = None
    ) -> Any:
        """Execute a handler through the Django-style middleware chain"""
        if not self.middlewares:
            # No middlewares, execute handler directly
            return await self._execute_sync_or_async_handler(event, handler, timeout)
        
        # Create Django-style middleware chain by wrapping the handler in middleware layers
        async def base_handler(event: 'BaseEvent[Any]') -> Any:
            return await self._execute_sync_or_async_handler(event, handler, timeout)
        
        # Wrap the handler with each middleware (in reverse order for correct execution)
        wrapped_handler = base_handler
        for middleware in reversed(self.middlewares):
            try:
                wrapped_handler = middleware(wrapped_handler)
            except Exception as e:
                # Log middleware initialization error and re-raise
                handler_id = get_handler_id(handler, self)
                logger.exception(
                    f'❌ {self} Error initializing middleware {middleware.__class__.__name__} '
                    f'for handler {get_handler_name(handler)}#{handler_id[-4:]}({event}) -> {type(e).__name__}({e})',
                    exc_info=True,
                )
                raise
        
        # Execute the wrapped handler
        return await wrapped_handler(event)

    async def _execute_sync_or_async_handler(
        self, event: 'BaseEvent[T_EventResultType]', handler: EventHandler, timeout: float | None = None
    ) -> Any:
        """Safely execute a single handler with deadlock detection"""

        # Check if this handler has already been executed for this event
        handler_id = get_handler_id(handler, self)

        logger.debug(f' ↳ {self}._execute_handler({event}, handler={get_handler_name(handler)}#{str(id(handler))[-4:]})')
        logger.debug(f'    Handler ID: {handler_id}')
        if handler_id in event.event_results:
            existing_result = event.event_results[handler_id]
            if existing_result.started_at is not None:
                raise RuntimeError(
                    f'Handler {get_handler_name(handler)}#{handler_id[-4:]} has already been executed for event {event.event_id}. '
                    f'Previous execution started at {existing_result.started_at}'
                )

        # Mark handler as started
        event_result = event.event_result_update(
            handler=handler, eventbus=self, status='started', timeout=timeout or event.event_timeout
        )

        # Set the current event in context so child events can reference it
        token = _current_event_context.set(event)
        # Mark that we're inside a handler
        handler_token = inside_handler_context.set(True)
        # Set the current handler ID so child events can be tracked
        handler_id_token = _current_handler_id_context.set(handler_id)

        # Create a task to monitor for potential deadlock / slow handlers
        async def deadlock_monitor():
            await asyncio.sleep(15.0)
            logger.warning(
                f'⚠️ {self} handler {get_handler_name(handler)}() has been running for >15s on event. Possible slow processing or deadlock.\n'
                '(handler could be trying to await its own result or could be blocked by another async task).\n'
                f'{get_handler_name(handler)}({event})'
            )

        monitor_task = asyncio.create_task(
            deadlock_monitor(), name=f'{self}.deadlock_monitor({event}, {get_handler_name(handler)}#{handler_id[-4:]})'
        )

        handler_task = None
        try:
            if inspect.iscoroutinefunction(handler):
                # Create a task for the handler so we can properly cancel it on timeout
                handler_task = asyncio.create_task(handler(event))  # type: ignore
                # This allows us to process child events when the handler awaits them
                result_value: Any = await asyncio.wait_for(handler_task, timeout=event_result.timeout)
            elif inspect.isfunction(handler) or inspect.ismethod(handler):
                # If handler function is sync function, run it directly in the main thread
                # This blocks but ensures we have access to the event loop, dont run it in a subthread!
                result_value: Any = handler(event)

                # If the sync handler returned a BaseEvent (from dispatch), DON'T await it
                # For forwarding handlers like bus.on('*', other_bus.dispatch), the handler
                # has already queued the event on the target bus. The event will be tracked
                # as a child event automatically.
                if isinstance(result_value, BaseEvent):
                    logger.debug(
                        f'Handler {get_handler_name(handler)} returned BaseEvent, not awaiting to avoid circular dependency'
                    )
            else:
                raise ValueError(f'Handler {get_handler_name(handler)} must be a sync or async function, got: {type(handler)}')

            logger.debug(
                f'    ↳ Handler {get_handler_name(handler)}#{handler_id[-4:]} returned: {type(result_value).__name__} {result_value}'  # pyright: ignore
            )
            # Cancel the monitor task since handler completed successfully
            monitor_task.cancel()

            # Record successful result
            event.event_result_update(handler=handler, eventbus=self, result=result_value)
            if handler_id in event.event_results:
                # logger.debug(
                #     f'    ↳ Updated result for {get_handler_name(handler)}#{handler_id[-4:]}: {event.event_results[handler_id].status}'
                # )
                pass
            else:
                logger.error(f'    ↳ ERROR: Result not found for {get_handler_name(handler)}#{handler_id[-4:]} after update!')
            return cast(T_EventResultType, result_value)

        except TimeoutError as e:
            # Cancel the monitor task on timeout too
            monitor_task.cancel()
            
            # Create a RuntimeError for timeout
            handler_timeout_error = RuntimeError(f'Event handler {get_handler_name(handler)}#{handler_id[-4:]}({event}) timed out after {event_result.timeout}s and interrupted any child processing')
            event.event_result_update(handler=handler, eventbus=self, error=handler_timeout_error)
            
            from bubus.logging import log_timeout_tree
            log_timeout_tree(event, event_result)
            raise e from handler_timeout_error
        except Exception as e:
            # Cancel the monitor task on error too
            monitor_task.cancel()

            # Record error
            event.event_result_update(handler=handler, eventbus=self, error=e)

            red = '\033[91m'
            reset = '\033[0m'
            logger.error(
                f'❌ {self} Error in event handler {get_handler_name(handler)}({event}) -> \n{red}{type(e).__name__}({e}){reset}\n{_log_filtered_traceback(e)}',
            )
            raise
        finally:
            # Reset context
            _current_event_context.reset(token)
            inside_handler_context.reset(handler_token)
            _current_handler_id_context.reset(handler_id_token)
            
            # Ensure handler task is cancelled if it's still running
            if handler_task and not handler_task.done():
                handler_task.cancel()
                try:
                    await asyncio.wait_for(handler_task, timeout=0.1)
                except (asyncio.CancelledError, TimeoutError):
                    pass  # Expected when we cancel the task
            
            # Ensure monitor task is cancelled
            try:
                if not monitor_task.done():
                    monitor_task.cancel()
                await monitor_task
            except asyncio.CancelledError:
                pass  # Expected when we cancel the monitor
            except Exception as e:
                # logger.debug(f"❌ {self} Handler monitor task cleanup error for {get_handler_name(handler)}#{str(id(handler))[-4:]}({event}): {type(e).__name__}: {e}")
                pass

    def _would_create_loop(self, event: 'BaseEvent[Any]', handler: EventHandler) -> bool:
        """Check if calling this handler would create a loop"""

        assert inspect.isfunction(handler) or inspect.iscoroutinefunction(handler) or inspect.ismethod(handler), (
            f'Handler {get_handler_name(handler)} must be a sync or async function, got: {type(handler)}'
        )

        # First check: If handler is another EventBus.dispatch method, check if we're forwarding to another bus that it's already been processed by
        if hasattr(handler, '__self__') and isinstance(handler.__self__, EventBus) and handler.__name__ == 'dispatch':  # pyright: ignore[reportFunctionMemberAccess]  # type: ignore
            target_bus = handler.__self__  # pyright: ignore[reportFunctionMemberAccess]  # type: ignore
            if target_bus.name in event.event_path:
                logger.debug(
                    f'⚠️ {self} handler {get_handler_name(handler)}#{str(id(handler))[-4:]}({event}) skipped to prevent infinite forwarding loop with {target_bus.name}'
                )
                return True

        # Second check: Check if there's already a result (pending or completed) for this handler on THIS bus
        # We use a combination of bus ID and handler ID to allow the same handler function
        # to run on different buses (important for forwarding)
        handler_id = get_handler_id(handler, self)
        if handler_id in event.event_results:
            existing_result = event.event_results[handler_id]
            if existing_result.status == 'pending' or existing_result.status == 'started':
                logger.debug(
                    f'⚠️ {self} handler {get_handler_name(handler)}#{str(id(handler))[-4:]}({event}) is already {existing_result.status} for event {event.event_id} (preventing recursive call)'
                )
                return True
            elif existing_result.completed_at is not None:
                logger.debug(
                    f'⚠️ {self} handler {get_handler_name(handler)}#{str(id(handler))[-4:]}({event}) already completed @ {existing_result.completed_at} for event {event.event_id} (will not re-run)'
                )
                return True

        # Third check: For non-forwarding handlers, check recursion depth
        # Forwarding handlers (EventBus.dispatch) are allowed to forward at any depth
        is_forwarding_handler = (
            inspect.ismethod(handler) and isinstance(handler.__self__, EventBus) and handler.__name__ == 'dispatch'
        )

        if not is_forwarding_handler:
            # Only check recursion for regular handlers, not forwarding
            recursion_depth = self._handler_dispatched_ancestor(event, handler_id)
            if recursion_depth > 2:
                raise RuntimeError(
                    f'Infinite loop detected: Handler {get_handler_name(handler)}#{str(id(handler))[-4:]} '
                    f'has recursively processed {recursion_depth} levels of events. '
                    f'Current event: {event}, Handler: {handler_id}'
                )
            elif recursion_depth == 2:
                logger.warning(
                    f'⚠️ {self} handler {get_handler_name(handler)}#{str(id(handler))[-4:]} '
                    f'at maximum recursion depth (2 levels) - next level will raise exception'
                )

        return False

    def _handler_dispatched_ancestor(
        self, event: 'BaseEvent[Any]', handler_id: str, visited: set[str] | None = None, depth: int = 0
    ) -> int:
        """Check how many times this handler appears in the ancestry chain. Returns the depth count."""
        # Prevent infinite recursion in case of circular parent references
        if visited is None:
            visited = set()
        if event.event_id in visited:
            return depth
        visited.add(event.event_id)

        # If this event has no parent, it's a root event - no ancestry to check
        if not event.event_parent_id:
            return depth

        # Find parent event in any bus's history
        parent_event = None
        # Create a list copy to avoid "Set changed size during iteration" error
        for bus in list(EventBus.all_instances):
            if event.event_parent_id in bus.event_history:
                parent_event = bus.event_history[event.event_parent_id]
                break

        if not parent_event:
            return depth

        # Check if this handler processed the parent event
        if handler_id in parent_event.event_results:
            result = parent_event.event_results[handler_id]
            if result.status in ('pending', 'started', 'completed'):
                # This handler processed the parent event, increment depth
                depth += 1

        # Recursively check the parent's ancestry
        return self._handler_dispatched_ancestor(parent_event, handler_id, visited, depth)

    async def _default_log_handler(self, event: 'BaseEvent[Any]') -> None:
        """Default handler that logs all events"""
        # logger.debug(
        # 	f'✅ {self} completed: {event} -> {list(event.event_results.values()) or '<no handlers matched>'}'
        # )
        pass

    # WAL functionality is now handled by WALEventBusMiddleware

    def cleanup_excess_events(self) -> int:
        """
        Clean up excess events from event_history based on max_history_size.

        Returns:
            Number of events removed from history
        """
        if not self.max_history_size or len(self.event_history) <= self.max_history_size:
            return 0

        # Sort events by creation time (oldest first)
        sorted_events = sorted(self.event_history.items(), key=lambda x: x[1].event_created_at.timestamp())

        # Remove oldest events to get down to max_history_size
        events_to_remove = sorted_events[: -self.max_history_size]
        event_ids_to_remove = [event_id for event_id, _ in events_to_remove]

        for event_id in event_ids_to_remove:
            del self.event_history[event_id]

        if event_ids_to_remove:
            logger.debug(f'🧹 {self} Cleaned up {len(event_ids_to_remove)} excess events from history')

        return len(event_ids_to_remove)

    def cleanup_event_history(self) -> int:
        """
        Clean up event history to maintain max_history_size limit.
        Prioritizes keeping pending/started events over completed ones.

        Returns:
            Total number of events removed from history
        """
        if not self.max_history_size or len(self.event_history) <= self.max_history_size:
            return 0

        # Separate events by status
        pending_events: list[tuple[str, 'BaseEvent[Any]']] = []
        started_events: list[tuple[str, 'BaseEvent[Any]']] = []
        completed_events: list[tuple[str, 'BaseEvent[Any]']] = []

        for event_id, event in self.event_history.items():
            if event.event_status == 'pending':
                pending_events.append((event_id, event))
            elif event.event_status == 'started':
                started_events.append((event_id, event))
            else:  # completed or error
                completed_events.append((event_id, event))

        # Sort completed events by creation time (oldest first)
        completed_events.sort(key=lambda x: x[1].event_created_at.timestamp())  # pyright: ignore[reportUnknownMemberType, reportUnknownLambdaType]

        # Calculate how many to remove
        total_events = len(self.event_history)
        events_to_remove_count = total_events - self.max_history_size

        events_to_remove: list[str] = []

        # First remove completed events (oldest first)
        if completed_events and events_to_remove_count > 0:
            remove_from_completed = min(len(completed_events), events_to_remove_count)
            events_to_remove.extend([event_id for event_id, _ in completed_events[:remove_from_completed]])
            events_to_remove_count -= remove_from_completed

        # If still need to remove more, remove oldest started events
        if events_to_remove_count > 0 and started_events:
            started_events.sort(key=lambda x: x[1].event_created_at.timestamp())  # pyright: ignore[reportUnknownMemberType, reportUnknownLambdaType]
            remove_from_started = min(len(started_events), events_to_remove_count)
            events_to_remove.extend([event_id for event_id, _ in started_events[:remove_from_started]])
            events_to_remove_count -= remove_from_started

        # If still need to remove more, remove oldest pending events
        if events_to_remove_count > 0 and pending_events:
            pending_events.sort(key=lambda x: x[1].event_created_at.timestamp())  # pyright: ignore[reportUnknownMemberType, reportUnknownLambdaType]
            events_to_remove.extend([event_id for event_id, _ in pending_events[:events_to_remove_count]])

        # Remove the events
        for event_id in events_to_remove:
            del self.event_history[event_id]

        if events_to_remove:
            logger.debug(
                f'🧹 {self} Cleaned up {len(events_to_remove)} events from history (kept {len(self.event_history)}/{self.max_history_size})'
            )

        return len(events_to_remove)

    def log_tree(self) -> None:
        """Print a nice pretty formatted tree view of all events in the history including their results and child events recursively"""
        from bubus.logging import log_eventbus_tree

        log_eventbus_tree(self)

    def _check_total_memory_usage(self) -> None:
        """Check total memory usage across all EventBus instances and warn if >50MB"""
        import sys

        total_bytes = 0
        bus_details: list[tuple[str, int, int, int]] = []

        # Iterate through all EventBus instances
        # Create a list copy to avoid "Set changed size during iteration" error
        for bus in list(EventBus.all_instances):
            try:
                bus_bytes = 0

                # Count events in history
                for event in bus.event_history.values():
                    bus_bytes += sys.getsizeof(event)
                    # Also count the event's data
                    if hasattr(event, '__dict__'):
                        for attr_value in event.__dict__.values():
                            if isinstance(attr_value, (str, bytes, list, dict)):
                                bus_bytes += sys.getsizeof(attr_value)  # pyright: ignore[reportUnknownArgumentType]

                # Count events in queue
                if bus.event_queue:
                    # Access internal queue storage
                    if hasattr(bus.event_queue, '_queue'):
                        queue: deque[BaseEvent] = bus.event_queue._queue  # type: ignore[attr-defined]
                        for event in queue:  # pyright: ignore[reportUnknownVariableType]
                            bus_bytes += sys.getsizeof(event)  # pyright: ignore[reportUnknownArgumentType]
                            if hasattr(event, '__dict__'):  # pyright: ignore[reportUnknownArgumentType]
                                for attr_value in event.__dict__.values():  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
                                    if isinstance(attr_value, (str, bytes, list, dict)):
                                        bus_bytes += sys.getsizeof(attr_value)  # pyright: ignore[reportUnknownArgumentType]

                total_bytes += bus_bytes
                bus_details.append(
                    (bus.name, bus_bytes, len(bus.event_history), bus.event_queue.qsize() if bus.event_queue else 0)
                )
            except Exception:
                # Skip buses that can't be measured
                continue

        total_mb = total_bytes / (1024 * 1024)

        if total_mb > 50:
            # Build detailed breakdown
            details: list[str] = []
            for name, bytes_used, history_size, queue_size in sorted(bus_details, key=lambda x: x[1], reverse=True):  # pyright: ignore[reportUnknownLambdaType]
                mb = bytes_used / (1024 * 1024)
                if mb > 0.1:  # Only show buses using >0.1MB
                    details.append(f'  - {name}: {mb:.1f}MB (history={history_size}, queue={queue_size})')

            warning_msg = (
                f'\n⚠️  WARNING: Total EventBus memory usage is {total_mb:.1f}MB (>50MB limit)\n'
                f'Active EventBus instances: {len(EventBus.all_instances)}\n'
            )
            if details:
                warning_msg += 'Memory breakdown:\n' + '\n'.join(details[:5])  # Show top 5
                if len(details) > 5:
                    warning_msg += f'\n  ... and {len(details) - 5} more'

            warning_msg += '\nConsider:\n'
            warning_msg += '  - Reducing max_history_size\n'
            warning_msg += '  - Clearing completed EventBus instances with stop(clear=True)\n'
            warning_msg += '  - Reducing event payload sizes\n'

            logger.warning(warning_msg)
