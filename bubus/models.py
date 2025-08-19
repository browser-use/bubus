import asyncio
import inspect
import logging
import os
import traceback
from collections.abc import Awaitable, Callable, Generator
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Generic, Literal, Protocol, Self, TypeAlias, cast, runtime_checkable
from uuid import UUID

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    TypeAdapter,
    field_serializer,
    model_validator,
)
from typing_extensions import TypeVar  # needed to get TypeVar(default=...) above python 3.11
from uuid_extensions import uuid7str

if TYPE_CHECKING:
    from bubus.service import EventBus


logger = logging.getLogger('bubus')

BUBUS_LOG_LEVEL = os.getenv('BUBUS_LOG_LEVEL', 'WARNING')  # WARNING normally, otherwise DEBUG when testing
LIBRARY_VERSION = os.getenv('LIBRARY_VERSION', '1.0.0')

logger.setLevel(BUBUS_LOG_LEVEL)


def validate_event_name(s: str) -> str:
    assert str(s).isidentifier() and not str(s).startswith('_'), f'Invalid event name: {s}'
    return str(s)


def validate_python_id_str(s: str) -> str:
    assert str(s).replace('.', '').isdigit(), f'Invalid Python ID: {s}'
    return str(s)


def validate_uuid_str(s: str) -> str:
    uuid = UUID(str(s))
    return str(uuid)


UUIDStr: TypeAlias = Annotated[str, AfterValidator(validate_uuid_str)]
PythonIdStr: TypeAlias = Annotated[str, AfterValidator(validate_python_id_str)]
PythonIdentifierStr: TypeAlias = Annotated[str, AfterValidator(validate_event_name)]
T_EventResultType = TypeVar('T_EventResultType', bound=Any, default=None)
# TypeVar for BaseEvent and its subclasses
# We use contravariant=True because if a handler accepts BaseEvent,
# it can also handle any subclass of BaseEvent
T_Event = TypeVar('T_Event', bound='BaseEvent[Any]', contravariant=True, default='BaseEvent[Any]')

# For protocols with __func__ attributes, we need an invariant TypeVar
T_EventInvariant = TypeVar('T_EventInvariant', bound='BaseEvent[Any]', default='BaseEvent[Any]')

# For handlers, we need to be flexible about the signature since:
# 1. Functions take just the event: handler(event)
# 2. Methods take self + event: handler(self, event)
# 3. Classmethods take cls + event: handler(cls, event)
# 4. Handlers can accept BaseEvent subclasses (contravariance)
#
# Python's type system doesn't handle this well, so we define specific protocols


@runtime_checkable
class EventHandlerFunc(Protocol[T_Event]):
    """Protocol for sync event handler functions"""

    def __call__(self, event: T_Event, /) -> Any: ...


@runtime_checkable
class AsyncEventHandlerFunc(Protocol[T_Event]):
    """Protocol for async event handler functions"""

    async def __call__(self, event: T_Event, /) -> Any: ...


@runtime_checkable
class EventHandlerMethod(Protocol[T_Event]):
    """Protocol for instance method event handlers"""

    def __call__(self, self_: Any, event: T_Event, /) -> Any: ...

    __self__: Any
    __name__: str


@runtime_checkable
class AsyncEventHandlerMethod(Protocol[T_Event]):
    """Protocol for async instance method event handlers"""

    async def __call__(self, self_: Any, event: T_Event, /) -> Any: ...

    __self__: Any
    __name__: str


@runtime_checkable
class EventHandlerClassMethod(Protocol[T_EventInvariant]):
    """Protocol for class method event handlers"""

    def __call__(self, cls: type[Any], event: T_EventInvariant, /) -> Any: ...

    __self__: type[Any]
    __name__: str
    __func__: Callable[[type[Any], T_EventInvariant], Any]


@runtime_checkable
class AsyncEventHandlerClassMethod(Protocol[T_EventInvariant]):
    """Protocol for async class method event handlers"""

    async def __call__(self, cls: type[Any], event: T_EventInvariant, /) -> Any: ...

    __self__: type[Any]
    __name__: str
    __func__: Callable[[type[Any], T_EventInvariant], Awaitable[Any]]


# Event handlers can be sync/async functions, methods, class methods, or coroutines
# The protocols are parameterized with BaseEvent but due to contravariance,
# they also accept handlers that take any BaseEvent subclass
EventHandler: TypeAlias = (
    EventHandlerFunc['BaseEvent[Any]']
    | AsyncEventHandlerFunc['BaseEvent[Any]']
    | EventHandlerMethod['BaseEvent[Any]']
    | AsyncEventHandlerMethod['BaseEvent[Any]']
    | EventHandlerClassMethod['BaseEvent[Any]']
    | AsyncEventHandlerClassMethod['BaseEvent[Any]']
    # | Callable[['BaseEvent'], Any]  # Simple sync callable
    # | Callable[['BaseEvent'], Awaitable[Any]]  # Simple async callable
    # | Coroutine[Any, Any, Any]  # Direct coroutine
)

# ContravariantEventHandler is needed to allow handlers to accept any BaseEvent subclass in some signatures
ContravariantEventHandler: TypeAlias = (
    EventHandlerFunc[T_Event]  # cannot be BaseEvent or type checker will complain
    | AsyncEventHandlerFunc['BaseEvent[Any]']
    | EventHandlerMethod['BaseEvent[Any]']
    | AsyncEventHandlerMethod[T_Event]  # cannot be 'BaseEvent' or type checker will complain
    | EventHandlerClassMethod['BaseEvent[Any]']
    | AsyncEventHandlerClassMethod['BaseEvent[Any]']
)

EventResultFilter = Callable[['EventResult[Any]'], bool]


def get_handler_name(handler: ContravariantEventHandler[T_Event]) -> str:
    assert hasattr(handler, '__name__'), f'Handler {handler} has no __name__ attribute!'
    if inspect.ismethod(handler):
        return f'{type(handler.__self__).__name__}.{handler.__name__}'
    elif callable(handler):
        return f'{handler.__module__}.{handler.__name__}'  # type: ignore
    else:
        raise ValueError(f'Invalid handler: {handler} {type(handler)}, expected a function, coroutine, or method')


def get_handler_id(handler: EventHandler, eventbus: Any = None) -> str:
    """Generate a unique handler ID based on the bus and handler instance."""
    if eventbus is None:
        return str(id(handler))
    return f'{id(eventbus)}.{id(handler)}'


def _extract_basemodel_generic_arg(cls: type) -> Any:
    """
    Extract T_EventResultType Generic arg from BaseModel[T_EventResultType] subclasses using pydantic generic metadata.
    Needed because pydantic messes with the mro and obscures the Generic from the bases list.
    https://github.com/pydantic/pydantic/issues/8410
    """
    # Direct check first for speed - most subclasses will have it directly
    if hasattr(cls, '__pydantic_generic_metadata__'):
        metadata: dict[str, Any] = cls.__pydantic_generic_metadata__  # type: ignore
        origin = metadata.get('origin')  # type: ignore
        args: tuple[Any, ...] = metadata.get('args')  # type: ignore
        if origin is BaseEvent and args and len(args) > 0:  # type: ignore
            return args[0]

    # Only check MRO if direct check failed
    # Skip first element (cls itself) since we already checked it
    for parent in cls.__mro__[1:]:
        if hasattr(parent, '__pydantic_generic_metadata__'):
            metadata = parent.__pydantic_generic_metadata__  # type: ignore
            # Check if this is a parameterized BaseEvent
            origin = metadata.get('origin')  # type: ignore
            args: tuple[Any, ...] = metadata.get('args')  # type: ignore
            if origin is BaseEvent and args and len(args) > 0:  # type: ignore
                return args[0]

    return None


class BaseEvent(BaseModel, Generic[T_EventResultType]):
    """
    The base model used for all Events that flow through the EventBus system.
    """

    model_config = ConfigDict(
        extra='allow',
        arbitrary_types_allowed=True,
        validate_assignment=True,
        validate_default=True,
        revalidate_instances='always',
    )

    # Class-level cache for auto-extracted event_result_type
    _event_result_type_cache: ClassVar[Any | None] = None

    event_type: PythonIdentifierStr = Field(default='UndefinedEvent', description='Event type name', max_length=64)
    event_schema: str = Field(
        default=f'UndefinedEvent@{LIBRARY_VERSION}',
        description='Event schema version in format ClassName@version',
        max_length=250,
    )  # long because it can include long function names / module paths
    event_timeout: float | None = Field(default=300.0, description='Timeout in seconds for event to finish processing')
    event_result_type: Any = Field(
        default=None, description='Type to cast/validate handler return values (e.g. int, str, bytes, BaseModel subclass)'
    )

    @field_serializer('event_result_type')
    def event_result_type_serializer(self, value: Any) -> str | None:
        """Serialize event_result_type to a string representation"""
        if value is None:
            return None
        # Use str() to get full representation: 'int', 'str', 'list[int]', etc.
        return str(value)

    # Runtime metadata
    event_id: UUIDStr = Field(default_factory=uuid7str, max_length=36)
    event_path: list[PythonIdentifierStr] = Field(default_factory=list, description='Path tracking for event routing')
    event_parent_id: UUIDStr | None = Field(
        default=None, description='ID of the parent event that triggered this event', max_length=36
    )

    # Completion tracking fields
    event_created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description='Timestamp when event was first dispatched to an EventBus aka marked pending',
    )
    event_processed_at: datetime | None = Field(
        default=None,
        description='Timestamp when event was first processed by any handler',
    )

    event_results: dict[PythonIdStr, 'EventResult[T_EventResultType]'] = Field(
        default_factory=dict, exclude=True
    )  # Results indexed by str(id(handler_func))

    # Completion signal
    _event_completed_signal: asyncio.Event | None = PrivateAttr(default=None)

    def __hash__(self) -> int:
        """Make events hashable using their unique event_id"""
        return hash(self.event_id)

    def __str__(self) -> str:
        """BaseEvent#ab12⏳"""
        icon = (
            '⏳'
            if self.event_status == 'pending'
            else '✅'
            if self.event_status == 'completed'
            else '❌'
            if self.event_status == 'error'
            else '🏃'
        )
        # AuthBus≫DataBus▶ AuthLoginEvent#ab12 ⏳
        return f'{"≫".join(self.event_path[1:] or "?")}▶ {self.event_type}#{self.event_id[-4:]} {icon}'

    def __await__(self) -> Generator[Self, Any, Any]:
        """Wait for event to complete and return self"""

        # long descriptive name here really helps make traceback easier to follow
        async def wait_for_handlers_to_complete_then_return_event():
            assert self.event_completed_signal is not None

            # If we're inside a handler and this event isn't complete yet,
            # we need to process it immediately to avoid deadlock
            from bubus.service import EventBus, holds_global_lock, inside_handler_context

            if not self.event_completed_signal.is_set() and inside_handler_context.get() and holds_global_lock.get():
                # We're inside a handler and hold the global lock
                # Process events until this one completes

                logger.debug(f'__await__ for {self} - inside handler context, processing child events')

                # Keep processing events from all buses until this event is complete
                max_iterations = 1000  # Prevent infinite loops
                iterations = 0

                try:
                    while not self.event_completed_signal.is_set() and iterations < max_iterations:
                        iterations += 1
                        processed_any = False

                        # Process any queued events on all buses
                        # Create a list copy to avoid "Set changed size during iteration" error
                        for bus in list(EventBus.all_instances):
                            if not bus or not bus.event_queue:
                                continue

                            # Process one event from this bus if available
                            try:
                                if bus.event_queue.qsize() > 0:
                                    event = bus.event_queue.get_nowait()
                                    await bus.process_event(event)
                                    bus.event_queue.task_done()
                                    processed_any = True
                                    # Check if the event we're waiting for is now complete
                                    if self.event_completed_signal.is_set():
                                        break
                            except asyncio.QueueEmpty:
                                pass
                        
                        # Break out of the loop if event completed after processing
                        if self.event_completed_signal.is_set():
                            break

                        if not processed_any:
                            # No events to process, yield control and check for cancellation
                            try:
                                await asyncio.sleep(0)
                            except asyncio.CancelledError:
                                raise
                except asyncio.CancelledError:
                    # Handler was cancelled due to timeout, exit cleanly
                    logger.debug(f'Polling loop cancelled for {self}')
                    raise

                if iterations >= max_iterations:
                    # logger.error(f'Max iterations reached while waiting for {self}')
                    pass
            else:
                # Not in handler context - wait for the event to complete normally
                await self.event_completed_signal.wait()

            # Check if any handlers had errors and raise the first one
            for result in self.event_results.values():
                if result.error:
                    raise result.error

            # Return the completed event without raising errors
            # Errors should only be raised when explicitly requested via event_result() methods
            return self

        return wait_for_handlers_to_complete_then_return_event().__await__()

    @model_validator(mode='before')
    @classmethod
    def _set_event_type_from_class_name(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Automatically set event_type to the class name if not provided"""
        is_class_default_unchanged = cls.model_fields['event_type'].default == 'UndefinedEvent'
        is_event_type_not_provided = 'event_type' not in data or data['event_type'] == 'UndefinedEvent'
        if is_class_default_unchanged and is_event_type_not_provided:
            data['event_type'] = cls.__name__
        return data

    @model_validator(mode='before')
    @classmethod
    def _set_event_schema_from_class_name(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Append the library version number to the event schema so we know what version was used to create any JSON dump"""
        is_class_default_unchanged = cls.model_fields['event_schema'].default == f'UndefinedEvent@{LIBRARY_VERSION}'
        is_event_schema_not_provided = 'event_schema' not in data or data['event_schema'] == f'UndefinedEvent@{LIBRARY_VERSION}'
        if is_class_default_unchanged and is_event_schema_not_provided:
            data['event_schema'] = f'{cls.__module__}.{cls.__qualname__}@{LIBRARY_VERSION}'
        return data

    @model_validator(mode='before')
    @classmethod
    def _set_event_result_type_from_generic_arg(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Automatically set event_result_type from Generic type parameter if not explicitly provided."""
        if not isinstance(data, dict):  # type: ignore
            return data

        # Fast path: if event_result_type is already in the data, skip all checks
        if 'event_result_type' in data:
            return data

        # Check if class explicitly defines event_result_type in model_fields
        # This handles cases where user explicitly sets event_result_type in class definition
        if 'event_result_type' in cls.model_fields:
            field = cls.model_fields['event_result_type']
            if field.default is not None and field.default != BaseEvent.model_fields['event_result_type'].default:
                # Explicitly set, use the default value
                data['event_result_type'] = field.default
                return data

        # Fast path: check if class has cached the result type
        if cls._event_result_type_cache is not None:
            data['event_result_type'] = cls._event_result_type_cache
            return data

        # Extract the generic type from BaseEvent[T]
        extracted_type = _extract_basemodel_generic_arg(cls)

        # Cache the result on the class
        cls._event_result_type_cache = extracted_type

        # Set the type if we successfully resolved it
        if extracted_type is not None:
            data['event_result_type'] = extracted_type

        return cast(dict[str, Any], data)  # type: ignore

    @property
    def event_completed_signal(self) -> asyncio.Event | None:
        """Lazily create asyncio.Event when accessed"""
        if self._event_completed_signal is None:
            try:
                asyncio.get_running_loop()
                self._event_completed_signal = asyncio.Event()
            except RuntimeError:
                pass  # Keep it None if no event loop
        return self._event_completed_signal

    @property
    def event_status(self) -> str:
        return 'completed' if self.event_completed_at else 'started' if self.event_started_at else 'pending'

    @property
    def event_children(self) -> list['BaseEvent[Any]']:
        """Get all child events dispatched from within this event's handlers"""
        children: list[BaseEvent[Any]] = []
        for event_result in self.event_results.values():
            children.extend(event_result.event_children)
        return children

    @property
    def event_started_at(self) -> datetime | None:
        """Timestamp when event first started being processed by any handler"""
        started_times = [result.started_at for result in self.event_results.values() if result.started_at is not None]
        # If no handlers but event was processed, use the processed timestamp
        if not started_times and self.event_processed_at:
            return self.event_processed_at
        return min(started_times) if started_times else None

    @property
    def event_completed_at(self) -> datetime | None:
        """Timestamp when event was completed by all handlers"""
        # If no handlers at all but event was processed, use the processed timestamp
        if not self.event_results and self.event_processed_at:
            return self.event_processed_at

        # All handlers must be done (completed or error)
        all_done = all(result.status in ('completed', 'error') for result in self.event_results.values())
        if not all_done:
            return None

        # Return the latest completion time
        completed_times = [result.completed_at for result in self.event_results.values() if result.completed_at is not None]
        return max(completed_times) if completed_times else self.event_processed_at

    @staticmethod
    def _event_result_is_truthy(event_result: 'EventResult[T_EventResultType]') -> bool:
        if event_result.status != 'completed':
            return False
        if event_result.result is None:
            return False
        if isinstance(event_result.result, BaseException) or event_result.error:
            return False
        if isinstance(
            event_result.result, BaseEvent
        ):  # omit if result is a BaseEvent, it's a forwarded event not an actual return value
            return False
        return True

    async def event_results_filtered(
        self,
        timeout: float | None = None,
        include: EventResultFilter = _event_result_is_truthy,
        raise_if_any: bool = True,
        raise_if_none: bool = True,
    ) -> 'dict[PythonIdStr, EventResult[T_EventResultType]]':
        """Get all results filtered by the include function"""

        # wait for all handlers to finish processing
        assert self.event_completed_signal is not None, 'EventResult cannot be awaited outside of an async context'
        await asyncio.wait_for(self.event_completed_signal.wait(), timeout=timeout or self.event_timeout)

        # Wait for each result to complete, but don't raise errors yet
        for event_result in self.event_results.values():
            try:
                await event_result
            except Exception:
                # Ignore exceptions here - we'll handle them based on raise_if_any below
                pass

        event_results: dict[PythonIdStr, EventResult[T_EventResultType]] = {
            handler_key: event_result for handler_key, event_result in self.event_results.items()
        }
        included_results: dict[PythonIdStr, EventResult[T_EventResultType]] = {
            handler_key: event_result for handler_key, event_result in event_results.items() if include(event_result)
        }
        error_results: dict[PythonIdStr, EventResult[T_EventResultType]] = {
            handler_key: event_result
            for handler_key, event_result in event_results.items()
            if event_result.error or isinstance(event_result.result, BaseException)
        }

        if raise_if_any and error_results:
            failing_handler, failing_result = list(error_results.items())[0]  # throw first error
            raise Exception(
                f'Event handler {failing_handler}({self}) returned an error -> {failing_result.error or cast(Any, failing_result.result)}'
            )

        if raise_if_none and not included_results:
            raise Exception(
                f'Expected at least one handler to return a non-None result, but none did! {self} -> {self.event_results}'
            )

        event_results_by_handler_id: dict[PythonIdStr, EventResult[T_EventResultType]] = {
            handler_key: result for handler_key, result in included_results.items()
        }
        for event_result in event_results_by_handler_id.values():
            assert event_result.result is not None, f'EventResult {event_result} has no result'

        return event_results_by_handler_id

    async def event_results_by_handler_id(
        self,
        timeout: float | None = None,
        include: EventResultFilter = _event_result_is_truthy,
        raise_if_any: bool = True,
        raise_if_none: bool = True,
    ) -> dict[PythonIdStr, T_EventResultType | None]:
        """Get all raw result values organized by handler id {handler1_id: handler1_result, handler2_id: handler2_result, ...}"""
        included_results = await self.event_results_filtered(
            timeout=timeout, include=include, raise_if_any=raise_if_any, raise_if_none=raise_if_none
        )
        return {
            handler_id: cast(T_EventResultType | None, event_result.result)
            for handler_id, event_result in included_results.items()
        }

    async def event_results_by_handler_name(
        self,
        timeout: float | None = None,
        include: EventResultFilter = _event_result_is_truthy,
        raise_if_any: bool = True,
        raise_if_none: bool = True,
    ) -> dict[PythonIdentifierStr, T_EventResultType | None]:
        """Get all raw result values organized by handler name {handler1_name: handler1_result, handler2_name: handler2_result, ...}"""
        included_results = await self.event_results_filtered(
            timeout=timeout, include=include, raise_if_any=raise_if_any, raise_if_none=raise_if_none
        )
        return {
            event_result.handler_name: cast(T_EventResultType | None, event_result.result)
            for event_result in included_results.values()
        }

    async def event_result(
        self,
        timeout: float | None = None,
        include: EventResultFilter = _event_result_is_truthy,
        raise_if_any: bool = True,
        raise_if_none: bool = True,
    ) -> T_EventResultType | None:
        """Get the first non-None result from the event handlers"""
        valid_results = await self.event_results_filtered(
            timeout=timeout, include=include, raise_if_any=raise_if_any, raise_if_none=raise_if_none
        )
        results = list(valid_results.values())
        return cast(T_EventResultType | None, results[0].result) if results else None

    async def event_results_list(
        self,
        timeout: float | None = None,
        include: EventResultFilter = _event_result_is_truthy,
        raise_if_any: bool = True,
        raise_if_none: bool = True,
    ) -> list[T_EventResultType | None]:
        """Get all result values in a list [handler1_result, handler2_result, ...]"""
        valid_results = await self.event_results_filtered(
            timeout=timeout, include=include, raise_if_any=raise_if_any, raise_if_none=raise_if_none
        )
        return [cast(T_EventResultType | None, event_result.result) for event_result in valid_results.values()]

    async def event_results_flat_dict(
        self,
        timeout: float | None = None,
        include: EventResultFilter = _event_result_is_truthy,
        raise_if_any: bool = True,
        raise_if_none: bool = False,
        raise_if_conflicts: bool = True,
    ) -> dict[str, Any]:
        """Assuming all handlers return dicts, merge all the returned dicts into a single flat dict {**handler1_result, **handler2_result, ...}"""

        valid_results = await self.event_results_filtered(
            timeout=timeout,
            include=lambda event_result: isinstance(event_result.result, dict) and include(event_result),
            raise_if_any=raise_if_any,
            raise_if_none=raise_if_none,
        )

        merged_results: dict[str, Any] = {}
        for event_result in valid_results.values():
            if not event_result.result:
                continue

            # check for event results trampling each other / conflicting
            overlapping_keys: set[str] = merged_results.keys() & event_result.result.keys()  # type: ignore
            if raise_if_conflicts and overlapping_keys:  # type: ignore
                raise Exception(
                    f'Event handler {event_result.handler_name} returned a dict with keys that would overwrite values from previous handlers: {overlapping_keys} (pass raise_if_conflicts=False to merge with last-handler-wins)'
                )  # type: ignore

            merged_results.update(
                event_result.result  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType]
            )  # update the merged dict with the contents of the result dict
        return merged_results

    async def event_results_flat_list(
        self,
        timeout: float | None = None,
        include: EventResultFilter = _event_result_is_truthy,
        raise_if_any: bool = True,
        raise_if_none: bool = True,
    ) -> list[Any]:
        """Assuming all handlers return lists, merge all the returned lists into a single flat list [*handler1_result, *handler2_result, ...]"""
        valid_results = await self.event_results_filtered(
            timeout=timeout,
            include=lambda event_result: isinstance(event_result.result, list) and include(event_result),
            raise_if_any=raise_if_any,
            raise_if_none=raise_if_none,
        )
        merged_results: list[T_EventResultType | None] = []
        for event_result in valid_results.values():
            merged_results.extend(
                cast(list[T_EventResultType | None], event_result.result)
            )  # append the contents of the list to the merged list
        return merged_results

    def event_result_update(
        self, handler: EventHandler, eventbus: 'EventBus | None' = None, **kwargs: Any
    ) -> 'EventResult[T_EventResultType]':
        """Create or update an EventResult for a handler"""

        from bubus.service import EventBus

        assert eventbus is None or isinstance(eventbus, EventBus)
        if eventbus is None and handler and inspect.ismethod(handler) and isinstance(handler.__self__, EventBus):
            eventbus = handler.__self__

        handler_name: str = get_handler_name(handler) if handler else 'unknown_handler'
        eventbus_id: PythonIdStr = str(id(eventbus) if eventbus is not None else '000000000000')
        eventbus_name: PythonIdentifierStr = str(eventbus and eventbus.name or 'EventBus')

        # Use bus+handler combination for unique ID
        handler_id: PythonIdStr = get_handler_id(handler, eventbus)

        # Get or create EventResult
        if handler_id not in self.event_results:
            self.event_results[handler_id] = cast(
                EventResult[T_EventResultType],
                EventResult(
                    event_id=self.event_id,
                    handler_id=handler_id,
                    handler_name=handler_name,
                    eventbus_id=eventbus_id,
                    eventbus_name=eventbus_name,
                    status=kwargs.get('status', 'pending'),
                    timeout=self.event_timeout,
                    result_type=self.event_result_type,
                ),
            )
            # logger.debug(f'Created EventResult for handler {handler_id}: {handler and get_handler_name(handler)}')

        # Update the EventResult with provided kwargs
        self.event_results[handler_id].update(**kwargs)
        # logger.debug(
        #     f'Updated EventResult for handler {handler_id}: status={self.event_results[handler_id].status}, total_results={len(self.event_results)}'
        # )
        # Don't mark complete here - let the EventBus do it after all handlers are done
        return self.event_results[handler_id]

    def event_mark_complete_if_all_handlers_completed(self) -> None:
        """Check if all handlers are done and signal completion"""
        if self.event_completed_signal and not self.event_completed_signal.is_set():
            # If there are no results at all, the event is complete
            if not self.event_results:
                if hasattr(self, 'event_processed_at'):
                    self.event_processed_at = datetime.now(UTC)
                self.event_completed_signal.set()
                return

            # Check if all handler results are done
            all_handlers_done = all(result.status in ('completed', 'error') for result in self.event_results.values())
            if not all_handlers_done:
                logger.debug(
                    f'Event {self} not complete - waiting for handlers: {[r for r in self.event_results.values() if r.status not in ("completed", "error")]}'
                )
                return

            # Recursively check if all child events are also complete
            if not self.event_are_all_children_complete():
                incomplete_children = [c for c in self.event_children if c.event_status != 'completed']
                logger.debug(
                    f'Event {self} not complete - waiting for {len(incomplete_children)} child events: {incomplete_children}'
                )
                return

            # All handlers and all child events are done
            if hasattr(self, 'event_processed_at'):
                self.event_processed_at = datetime.now(UTC)
            logger.debug(f'Event {self} marking complete - all handlers and children done')
            self.event_completed_signal.set()

    def event_are_all_children_complete(self, _visited: set[str] | None = None) -> bool:
        """Recursively check if all child events and their descendants are complete"""
        if _visited is None:
            _visited = set()

        # Prevent infinite recursion on circular references
        if self.event_id in _visited:
            return True
        _visited.add(self.event_id)

        for child_event in self.event_children:
            if child_event.event_status != 'completed':
                logger.debug(f'Event {self} has incomplete child {child_event}')
                return False
            # Recursively check child's children
            if not child_event.event_are_all_children_complete(_visited):
                return False
        return True

    def event_log_safe_summary(self) -> dict[str, Any]:
        """only event metadata without contents, avoid potentially sensitive event contents in logs"""
        return {k: v for k, v in self.model_dump(mode='json').items() if k.startswith('event_') and 'results' not in k}

    def event_log_tree(
        self,
        indent: str = '',
        is_last: bool = True,
        child_events_by_parent: 'dict[str | None, list[BaseEvent[Any]]] | None' = None,
    ) -> None:
        """Print this event and its results with proper tree formatting"""
        from bubus.logging import log_event_tree

        log_event_tree(self, indent, is_last, child_events_by_parent)

    @property
    def event_bus(self) -> 'EventBus':
        """Get the EventBus that is currently processing this event"""
        from bubus.service import EventBus, inside_handler_context

        if not inside_handler_context.get():
            raise RuntimeError('event_bus property can only be accessed from within an event handler')

        # The event_path contains all buses this event has passed through
        # The last one in the path is the one currently processing
        if not self.event_path:
            raise RuntimeError('Event has no event_path - was it dispatched?')

        current_bus_name = self.event_path[-1]

        # Find the bus by name
        # Create a list copy to avoid "Set changed size during iteration" error
        for bus in list(EventBus.all_instances):
            if bus and hasattr(bus, 'name') and bus.name == current_bus_name:
                return bus

        raise RuntimeError(f'Could not find active EventBus named {current_bus_name}')


def attr_name_allowed(key: str) -> bool:
    return key in pydantic_builtin_attrs or key in event_builtin_attrs or key.startswith('_')


# PSA: All BaseEvent buil-in attrs and methods must be prefixed with "event_" in order to avoid clashing with data contents (which share a namespace with the metadata)
# This is the same approach Pydantic uses for their special `model_*` attrs (and BaseEvent is also a pydantic model, so model_ prefixes are reserved too)
# resist the urge to nest the event data in an inner object unless absolutely necessary, flat simplifies most of the code and makes it easier to read JSON logs with less nesting
pydantic_builtin_attrs = dir(BaseModel)
event_builtin_attrs = {key for key in dir(BaseEvent) if key.startswith('event_')}
illegal_attrs = {key for key in dir(BaseEvent) if not attr_name_allowed(key)}
assert not illegal_attrs, (
    'All BaseEvent attrs and methods must be prefixed with "event_" in order to avoid clashing '
    'with BaseEvent subclass fields used to store event contents (which share a namespace with the event_ metadata). '
    f'not allowed: {illegal_attrs}'
)


class EventResult(BaseModel, Generic[T_EventResultType]):
    """Individual result from a single handler"""

    model_config = ConfigDict(
        extra='forbid',
        arbitrary_types_allowed=True,
        validate_assignment=False,  # Disable to allow flexible result types - validation handled in update()
        validate_default=True,
        revalidate_instances='always',
    )

    # Automatically set fields, setup at Event init and updated by the EventBus._execute_sync_or_async_handler() calling event_result.update(...)
    id: UUIDStr = Field(default_factory=uuid7str)
    status: Literal['pending', 'started', 'completed', 'error'] = 'pending'
    event_id: UUIDStr
    handler_id: PythonIdStr
    handler_name: str
    result_type: Any | type[T_EventResultType] | None = None
    eventbus_id: PythonIdStr
    eventbus_name: PythonIdentifierStr
    timeout: float | None = None
    started_at: datetime | None = None

    # Result fields, updated by the EventBus._execute_sync_or_async_handler() calling event_result.update(...)
    result: T_EventResultType | BaseEvent[Any] | None = None
    error: BaseException | None = None
    completed_at: datetime | None = None

    # Completion signal
    _handler_completed_signal: asyncio.Event | None = PrivateAttr(default=None)

    # any child events that were emitted during handler execution are captured automatically and stored here to track hierarchy
    # note about why this is BaseEvent[Any] instead of a more specific type:
    #   unfortunately we cant determine child event types statically / it's not worth it to force child event types to be defined at compile-time
    #   so we just allow handlers to emit any BaseEvent subclass/instances with any result types
    #   in theory it's possible to define the entire event tree hierarchy at compile-time with something like ParentEvent[ChildEvent[GrandchildEvent[FinalResultValueType]]],
    #   it's not worth the complexity headache it would incur on users of the library though,
    #   and it would significantly reduce runtime flexibility, e.g. you couldn't define and dispatch arbitrary server-provided event types at runtime
    event_children: list['BaseEvent[Any]'] = Field(default_factory=list)  # pyright: ignore[reportUnknownVariableType]

    @property
    def handler_completed_signal(self) -> asyncio.Event | None:
        """Lazily create asyncio.Event when accessed"""
        if self._handler_completed_signal is None:
            try:
                asyncio.get_running_loop()
                self._handler_completed_signal = asyncio.Event()
            except RuntimeError:
                pass  # Keep it None if no event loop
        return self._handler_completed_signal

    def __str__(self) -> str:
        handler_qualname = f'{self.eventbus_name}.{self.handler_name}'
        return f'{handler_qualname}() -> {self.result or self.error or "..."} ({self.status})'

    def __repr__(self) -> str:
        icon = '🏃' if self.status == 'pending' else '✅' if self.status == 'completed' else '❌'
        return f'{self.handler_name}#{self.handler_id[-4:]}() {icon}'

    def __await__(self) -> Generator[Self, Any, T_EventResultType | BaseEvent[Any] | None]:
        """
        Wait for this result to complete and return the result or raise error.
        Does not execute the handler itself, only waits for it to be marked completed by the EventBus.
        EventBus triggers handlers and calls event_result.update() to mark them as started or completed.
        """

        async def wait_for_handler_to_complete_and_return_result() -> T_EventResultType | BaseEvent[Any] | None:
            assert self.handler_completed_signal is not None, 'EventResult cannot be awaited outside of an async context'

            try:
                await asyncio.wait_for(self.handler_completed_signal.wait(), timeout=self.timeout)
            except TimeoutError:
                # self.handler_completed_signal.clear()
                raise RuntimeError(f'Event handler {self.eventbus_name}.{self.handler_name}(#{self.event_id[-4:]}) timed out after {self.timeout}s')

            if self.status == 'error' and self.error:
                raise self.error if isinstance(self.error, BaseException) else Exception(self.error)  # pyright: ignore[reportUnnecessaryIsInstance]

            return self.result

        return wait_for_handler_to_complete_and_return_result().__await__()

    def update(self, **kwargs: Any) -> Self:
        """Update the EventResult with provided kwargs, called by EventBus during handler execution."""

        # fix common mistake of returning an exception object instead of marking the event result as an error result
        if 'result' in kwargs and isinstance(kwargs['result'], BaseException):
            logger.warning(
                f'ℹ Event handler {self.handler_name} returned an exception object, auto-converting to EventResult(result=None, status="error", error={kwargs["result"]})'
            )
            kwargs['error'] = kwargs['result']
            kwargs['status'] = 'error'
            kwargs['result'] = None

        if 'result' in kwargs:
            result: Any = kwargs['result']
            self.status = 'completed'
            if self.result_type is not None and result is not None:
                # Always allow BaseEvent results without validation
                # This is needed for event forwarding patterns like bus1.on('*', bus2.dispatch)
                if isinstance(result, BaseEvent):
                    self.result = cast(T_EventResultType, result)
                else:
                    # cast the return value to the expected type using TypeAdapter
                    try:
                        if issubclass(self.result_type, BaseModel):
                            # if expected result type is a pydantic model, validate it with pydantic
                            validated_result = self.result_type.model_validate(result)
                        else:
                            # cast the return value to the expected type e.g. int(result) / str(result) / list(result) / etc.
                            ResultType = TypeAdapter(self.result_type)
                            validated_result = ResultType.validate_python(result)

                        # Normal assignment works, make sure validate_assignment=False otherwise pydantic will attempt to re-validate it a second time
                        self.result = cast(T_EventResultType, validated_result)

                    except Exception as cast_error:
                        self.error = ValueError(
                            f'Event handler returned a value that did not match expected event_result_type: {self.result_type.__name__}({result}) -> {type(cast_error).__name__}: {cast_error}'
                        )
                        self.result = None
                        self.status = 'error'
            else:
                # No result_type specified or result is None - assign directly
                self.result = cast(T_EventResultType, result)

        if 'error' in kwargs:
            assert isinstance(kwargs['error'], (BaseException, str)), (
                f'Invalid error type: {type(kwargs["error"]).__name__} {kwargs["error"]}'
            )
            self.error = kwargs['error'] if isinstance(kwargs['error'], BaseException) else Exception(kwargs['error'])  # pyright: ignore[reportUnnecessaryIsInstance]
            self.status = 'error'

        if 'status' in kwargs:
            assert kwargs['status'] in ('pending', 'started', 'completed', 'error'), f'Invalid status: {kwargs["status"]}'
            self.status = kwargs['status']

        if self.status != 'pending' and not self.started_at:
            self.started_at = datetime.now(UTC)

        if self.status in ('completed', 'error') and not self.completed_at:
            self.completed_at = datetime.now(UTC)
            if self.handler_completed_signal:
                self.handler_completed_signal.set()
        return self

    def log_tree(
        self, indent: str = '', is_last: bool = True, child_events_by_parent: dict[str | None, list[BaseEvent[Any]]] | None = None
    ) -> None:
        """Print this result and its child events with proper tree formatting"""
        from bubus.logging import log_eventresult_tree

        log_eventresult_tree(self, indent, is_last, child_events_by_parent)


# Analytics events are now in bubus.middleware module


# Resolve forward references
BaseEvent.model_rebuild()
EventResult.model_rebuild()
