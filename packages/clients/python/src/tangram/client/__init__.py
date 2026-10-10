"""The standalone Tangram HTTP client."""

from __future__ import annotations

import asyncio
import errno
import os
import random
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any, Self, TypedDict, Unpack, cast, overload

from ..config import (
    Config as StdioConfig,
)
from ..config import (
    HttpConfig,
    RetryOptions,
    compatibility_date,
    default_config,
    default_http_config,
    default_retry_options,
    validate_config,
    validate_http_config,
    validate_retry_options,
)
from ..config import version as default_version
from ..error import Error, ErrorData
from ..http import Body, Request, Response, Stream
from ..http.coalesce import coalesce
from ..http2 import Session, TransportError
from ..object import Object
from ..progress import Event, progress
from ..progress import last_output as last_output
from ..referent import Referent

if TYPE_CHECKING:
    from ..location import ArgObject as LocationArgObject
    from ..location import LocationObject
    from ..object import ObjectData
    from ..process import ProcessDataObject
    from ..process.outcome import ProcessOutcome
    from ..process.stdio import ReadArgObject, StdioChunk
    from ..sandbox import SandboxOutput
    from ..value import ValueData, ValueType
    from .checkin import ArgObject as CheckinArg
    from .checkin import Options as CheckinOptions
    from .checkin import Output as CheckinOutput
    from .checkout import ArgObject as CheckoutArg
    from .checkout import KeywordOptions as CheckoutKeywordOptions
    from .checkout import Output as CheckoutOutput
    from .process.cancel import Cancel
    from .process.cancel import KeywordOptions as CancelKeywordOptions
    from .process.connect import (
        ClientMessage,
        ConnectArgObject,
        Header,
        ServerMessage,
        TtyArg,
    )
    from .process.get import Get as ProcessGet
    from .process.put import KeywordOptions as PutKeywordOptions
    from .process.put import Put as ProcessPut
    from .process.signal import KeywordOptions as SignalKeywordOptions
    from .process.signal import Signal
    from .process.spawn import ArgObject as SpawnArg
    from .process.spawn import OutputObject as SpawnOutput
    from .process.stdio.read import KeywordOptions as StdioReadKeywordOptions
    from .process.stdio.write import KeywordOptions as StdioWriteKeywordOptions
    from .process.tty.put import KeywordOptions as TtyKeywordOptions
    from .process.wait import Wait
    from .read import KeywordOptions as ReadKeywordOptions
    from .read import Read
    from .sandbox.create import DataArgObject as SandboxCreateArg
    from .sandbox.destroy import ArgObject as SandboxDestroyArg
    from .sandbox.get import ArgObject as SandboxGetArg
    from .write import Write


class ObjectPutOutput(TypedDict):
    object: Referent[str]


class ObjectBatchOutput(TypedDict):
    objects: list[Referent[str]]


class RequestError(Exception):
    def __init__(self, source: Exception):
        self.source = source
        super().__init__(str(source))


async def retry[T](
    options: RetryOptions,
    function: Callable[[], Awaitable[T]],
    should_retry: Callable[[Exception], bool] = lambda error: True,
) -> T:
    for attempt in range(options["max_retries"] + 1):
        try:
            return await function()
        except Exception as error:
            if attempt == options["max_retries"] or not should_retry(error):
                raise
            multiplier = 2 ** min(attempt + 1, 31)
            jitter = random.random() * options["jitter"]
            delay = min(options["backoff"] * multiplier + jitter, options["max_delay"])
            await asyncio.sleep(delay)
    raise AssertionError("unreachable")


class Client:
    def __init__(
        self,
        *,
        url: str | None = None,
        token: str | None = None,
        stdio: StdioConfig | None = None,
        http: HttpConfig | None = None,
        reconnect: RetryOptions | None = None,
        retry: RetryOptions | None = None,
        version: str | None = None,
    ):
        self.stdio = stdio if stdio is not None else default_config()
        validate_config(self.stdio)
        self.http = http if http is not None else default_http_config()
        validate_http_config(self.http, self.stdio)
        self.reconnect = reconnect if reconnect is not None else default_retry_options()
        self.retry = retry if retry is not None else default_retry_options()
        validate_retry_options(self.reconnect)
        validate_retry_options(self.retry)
        self.version = version if version is not None else default_version
        self.url = url
        self.token = token
        self._session: Session | None = None
        self._connecting: asyncio.Task[Session] | None = None

    def arg(self) -> dict[str, str]:
        return {
            name: value
            for name in ("token", "url")
            if (
                value := getattr(self, name)
                if getattr(self, name) is not None
                else os.environ.get(f"TANGRAM_{name.upper()}")
            )
            is not None
            and isinstance(value, str)
        }

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()

    async def close(self) -> None:
        if self._connecting is not None:
            self._connecting.cancel()
            await asyncio.gather(self._connecting, return_exceptions=True)
            self._connecting = None
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _connect(self) -> Session:
        while True:
            session = self._session
            if session is not None:
                if not session.closed:
                    return session
                self._disconnect(session)
                await session.close()
            connecting = self._connecting
            if connecting is None:
                connecting = asyncio.create_task(
                    retry(self.reconnect, self._create_session)
                )
                self._connecting = connecting
            try:
                next_session = await asyncio.shield(connecting)
            finally:
                if connecting.done() and self._connecting is connecting:
                    self._connecting = None
            if next_session.closed:
                await next_session.close()
                continue
            if self._session is None:
                self._session = next_session
            elif self._session is not next_session:
                await next_session.close()
            return self._session

    async def _create_session(self) -> Session:
        url = self.url if self.url is not None else os.environ.get("TANGRAM_URL")
        if not isinstance(url, str):
            raise ValueError("missing TANGRAM_URL")
        from .. import host

        return await host.http2.Session.connect(url, self.http["http2"])

    def _disconnect(self, session: Session | None = None) -> None:
        if session is not None and self._session is not session:
            return
        self._session = None

    @staticmethod
    def object_id(data: ObjectData) -> str:
        from ..host import object_id

        return object_id(data)

    @staticmethod
    def parse_value(value: str) -> ValueData:
        from ..host import parse_value

        return parse_value(value)

    @staticmethod
    def stringify_value(value: ValueData) -> str:
        from ..host import stringify_value

        return stringify_value(value)

    async def send(self, request: Request) -> Response:
        try:
            return await self._send(request)
        except RequestError as error:
            raise error.source from None

    async def send_with_retry(self, request: Request) -> Response:
        if request.body is not None and not request.body.replayable:
            raise ValueError("cannot retry a request with a streaming body")
        try:
            return await retry(
                self.retry,
                lambda: self._send(request),
                lambda error: (
                    isinstance(error, RequestError) and is_retryable_error(error.source)
                ),
            )
        except RequestError as error:
            raise error.source from None

    async def _send(self, request: Request) -> Response:
        async with asyncio.timeout(60):
            return await self._send_inner(request)

    async def _send_inner(self, request: Request) -> Response:
        token = (
            self.token if self.token is not None else os.environ.get("TANGRAM_TOKEN")
        )
        if token is not None and not isinstance(token, str):
            raise ValueError("invalid TANGRAM_TOKEN")
        headers = dict(request.headers)
        headers.setdefault("x-tg-compatibility-date", compatibility_date)
        headers.setdefault("x-tg-version", self.version)
        if token is not None and "authorization" not in headers:
            headers["authorization"] = f"Bearer {token}"
        body = (
            None
            if request.body is None
            else Body(coalesce(request.body, self.http["coalescing_target_size"]))
        )
        request = Request(request.method, request.uri, headers, body)
        session = await self._connect()
        try:
            return await session.send(request)
        except Exception as error:
            if is_retryable_error(error):
                self._disconnect(session)
                session.retire()
            raise RequestError(error) from error

    async def _request(
        self, method, path, *, arg=None, data=None, missing=False, statuses=()
    ):
        headers = {"accept": "application/json"}
        body = None
        if data is not None:
            headers["content-type"] = "application/json"
            body = Body.json(data)
        request = Request(method, path, headers, body)
        if arg is not None:
            request.arg(wire_arg(arg))
        response = await self.send_with_retry(request)
        if missing and response.status == 404:
            await response.collect()
            return None
        if response.status not in statuses:
            await check_response(response)
        return response

    async def _json(self, method: str, path: str, **options) -> object:
        response = await self._request(method, path, **options)
        return None if response is None else response_locations(await response.json())

    async def try_get_object(
        self,
        id: str,
        arg: Object.Get.Arg | None = None,
        **options: Unpack[Object.Get.Arg],
    ) -> Object.Get.Output | None:
        from .object.get import try_get_object

        return await try_get_object(self, id, **_options(arg, options))

    async def get_object(
        self,
        id: str,
        arg: Object.Get.Arg | None = None,
        **options: Unpack[Object.Get.Arg],
    ) -> Object.Get.Output:
        from .object.get import get_object

        return await get_object(self, id, **_options(arg, options))

    async def put_object(
        self,
        id: str,
        arg: Object.Put.Arg | ObjectData,
        *,
        children: list[Referent[str]] | None = None,
        location: LocationArgObject | LocationObject | str | None = None,
    ) -> Object.Put.Output:
        from .object.put import put_object

        if "data" in arg:
            return await put_object(self, id, **cast("Object.Put.Arg", arg))
        return await put_object(self, id, arg, children=children, location=location)

    async def post_object_batch(
        self,
        arg: Object.Batch.Arg | list[Object.Batch.Object],
        *,
        location: LocationArgObject | LocationObject | str | None = None,
    ) -> Object.Batch.Output:
        from .object.batch import post_object_batch

        if isinstance(arg, dict):
            return await post_object_batch(self, **arg)
        return await post_object_batch(self, arg, location=location)

    @overload
    async def write(self, arg_or_bytes: str | bytes) -> str: ...

    @overload
    async def write(
        self, arg_or_bytes: Write.Arg, input: AsyncIterable[bytes]
    ) -> Write.Output: ...

    async def write(
        self,
        arg_or_bytes: Write.Arg | str | bytes,
        input: AsyncIterable[bytes] | None = None,
    ) -> Write.Output | str:
        from .write import write

        if isinstance(arg_or_bytes, (str, bytes)):
            return await write(self, arg_or_bytes)
        if input is None:
            raise TypeError("a byte stream is required")
        return await write(self, arg_or_bytes, input)

    async def try_read_stream(
        self, arg: Read.Arg | str, **options: Unpack[ReadKeywordOptions]
    ) -> Stream[Read.Event.ChunkEvent | Read.Event.End] | None:
        from .read import try_read_stream

        if isinstance(arg, dict):
            return await try_read_stream(self, **_options(arg, options))
        return await try_read_stream(self, arg, **options)

    async def try_read(
        self, arg: Read.Arg | str, **options: Unpack[ReadKeywordOptions]
    ) -> bytes | None:
        from .read import try_read

        if isinstance(arg, dict):
            return await try_read(self, **_options(arg, options))
        return await try_read(self, arg, **options)

    async def read(
        self, arg: Read.Arg | str, **options: Unpack[ReadKeywordOptions]
    ) -> bytes:
        from .read import read

        if isinstance(arg, dict):
            return await read(self, **_options(arg, options))
        return await read(self, arg, **options)

    async def _progress[T](
        self, path: str, data: Any, convert: Callable[[Any], T] = lambda value: value
    ) -> Stream[Event[T]]:
        request = Request(
            "POST",
            path,
            {"accept": "text/event-stream", "content-type": "application/json"},
            Body.json(data),
        )
        response = await self.send_with_retry(request)
        await check_response(response)
        return Stream(progress(response, convert), response.close)

    async def checkin(
        self,
        arg: CheckinArg | str,
        *,
        options: CheckinOptions | None = None,
        updates: list[str] | None = None,
    ) -> Stream[Event[CheckinOutput]]:
        from .checkin import checkin

        if isinstance(arg, dict):
            return await checkin(self, arg)
        return await checkin(self, arg, options=options, updates=updates)

    async def checkout(
        self,
        arg: CheckoutArg | list[Referent[str] | str | Object],
        **options: Unpack[CheckoutKeywordOptions],
    ) -> Stream[Event[CheckoutOutput]]:
        from .checkout import checkout

        if isinstance(arg, dict):
            return await checkout(self, **_options(arg, options))
        return await checkout(self, arg, **options)

    async def try_get_process(
        self,
        id: str,
        arg: ProcessGet.Arg | None = None,
        **options: Unpack[ProcessGet.Arg],
    ) -> ProcessGet.Output | None:
        from .process.get import try_get_process

        return await try_get_process(self, id, **_options(arg, options))

    async def get_process(
        self,
        id: str,
        arg: ProcessGet.Arg | None = None,
        **options: Unpack[ProcessGet.Arg],
    ) -> ProcessGet.Output:
        from .process.get import get_process

        return await get_process(self, id, **_options(arg, options))

    async def put_process(
        self,
        id: str,
        arg: ProcessPut.Arg | ProcessDataObject,
        **options: Unpack[PutKeywordOptions],
    ) -> ProcessPut.Output:
        from .process.put import put_process

        if "data" in arg:
            return await put_process(self, id, **_options(arg, options))
        return await put_process(self, id, arg, **options)

    async def try_spawn_process(
        self, arg: SpawnArg
    ) -> Stream[Event[SpawnOutput | None]]:
        from .process.spawn import try_spawn_process

        return await try_spawn_process(self, arg)

    async def spawn_process(self, arg: SpawnArg) -> AsyncIterator[Event[SpawnOutput]]:
        from .process.spawn import spawn_process

        return await spawn_process(self, arg)

    async def try_cancel_process(
        self,
        id: str,
        arg: Cancel.Arg | None = None,
        **options: Unpack[CancelKeywordOptions],
    ) -> Cancel.Output | None:
        from .process.cancel import try_cancel_process

        return await try_cancel_process(self, id, **_options(arg, options))

    async def cancel_process(
        self,
        id: str,
        arg: Cancel.Arg | None = None,
        **options: Unpack[CancelKeywordOptions],
    ) -> Cancel.Output:
        from .process.cancel import cancel_process

        return await cancel_process(self, id, **_options(arg, options))

    async def try_signal_process(
        self,
        id: str,
        arg: Signal.Arg | None = None,
        **options: Unpack[SignalKeywordOptions],
    ) -> bool | None:
        from .process.signal import try_signal_process

        return await try_signal_process(self, id, **_options(arg, options))

    async def signal_process(
        self,
        id: str,
        arg: Signal.Arg | None = None,
        **options: Unpack[SignalKeywordOptions],
    ) -> None:
        from .process.signal import signal_process

        return await signal_process(self, id, **_options(arg, options))

    async def try_set_process_tty_size(
        self, id: str, arg: TtyArg | None = None, **options: Unpack[TtyKeywordOptions]
    ) -> bool | None:
        from .process.tty.put import try_set_process_tty_size

        return await try_set_process_tty_size(self, id, **_options(arg, options))

    async def set_process_tty_size(
        self, id: str, arg: TtyArg | None = None, **options: Unpack[TtyKeywordOptions]
    ) -> None:
        from .process.tty.put import set_process_tty_size

        return await set_process_tty_size(self, id, **_options(arg, options))

    async def try_wait_process_promise(
        self, id: str, arg: Wait.Arg | None = None, **options: Unpack[Wait.Arg]
    ) -> Callable[[], Awaitable[ProcessOutcome[ValueType]]] | None:
        from .process.wait import try_wait_process_promise

        return await try_wait_process_promise(self, id, **_options(arg, options))

    async def wait_process_promise(
        self, id: str, arg: Wait.Arg | None = None, **options: Unpack[Wait.Arg]
    ) -> Callable[[], Awaitable[ProcessOutcome[ValueType]]]:
        from .process.wait import wait_process_promise

        return await wait_process_promise(self, id, **_options(arg, options))

    async def wait_process(
        self, id: str, arg: Wait.Arg | None = None, **options: Unpack[Wait.Arg]
    ) -> ProcessOutcome[ValueType]:
        from .process.wait import wait_process

        return await wait_process(self, id, **_options(arg, options))

    async def connect_process(
        self, arg: ConnectArgObject, input: AsyncIterable[ClientMessage]
    ) -> tuple[Header, Stream[ServerMessage]]:
        from .process.connect import connect_process

        return await connect_process(self, arg, input)

    async def try_read_process_stdio(
        self,
        id: str,
        arg: ReadArgObject | None = None,
        **options: Unpack[StdioReadKeywordOptions],
    ) -> Stream[StdioChunk] | None:
        from .process.stdio.read import try_read_process_stdio

        return await try_read_process_stdio(self, id, **_options(arg, options))

    async def try_write_process_stdio(
        self,
        id: str,
        arg: StdioWriteKeywordOptions | AsyncIterable[StdioChunk],
        input: AsyncIterable[StdioChunk] | None = None,
        complete: Callable[[StdioChunk], None] | None = None,
        **options: Unpack[StdioWriteKeywordOptions],
    ) -> bool | None:
        from .process.stdio.write import try_write_process_stdio

        resolved_options: dict[str, Any] = dict(options)
        if isinstance(arg, dict):
            resolved_options = _options(arg, options)
        else:
            input = cast("AsyncIterable[StdioChunk]", arg)
        if complete is not None:
            resolved_options["complete"] = complete
        return await try_write_process_stdio(self, id, input, **resolved_options)

    async def write_process_stdio(
        self,
        id: str,
        arg: StdioWriteKeywordOptions | AsyncIterable[StdioChunk],
        input: AsyncIterable[StdioChunk] | None = None,
        complete: Callable[[StdioChunk], None] | None = None,
        **options: Unpack[StdioWriteKeywordOptions],
    ) -> None:
        from .process.stdio.write import write_process_stdio

        resolved_options: dict[str, Any] = dict(options)
        if isinstance(arg, dict):
            resolved_options = _options(arg, options)
        else:
            input = cast("AsyncIterable[StdioChunk]", arg)
        if complete is not None:
            resolved_options["complete"] = complete
        return await write_process_stdio(self, id, input, **resolved_options)

    async def create_sandbox(self, arg: SandboxCreateArg) -> SandboxOutput:
        from .sandbox.create import create_sandbox

        return await create_sandbox(self, arg)

    async def try_get_sandbox(
        self,
        id: str,
        arg: SandboxGetArg | None = None,
        **options: Unpack[SandboxGetArg],
    ) -> SandboxOutput | None:
        from .sandbox.get import try_get_sandbox

        return await try_get_sandbox(self, id, **_options(arg, options))

    async def get_sandbox(
        self,
        id: str,
        arg: SandboxGetArg | None = None,
        **options: Unpack[SandboxGetArg],
    ) -> SandboxOutput:
        from .sandbox.get import get_sandbox

        return await get_sandbox(self, id, **_options(arg, options))

    async def try_destroy_sandbox(
        self,
        id: str,
        arg: SandboxDestroyArg | None = None,
        **options: Unpack[SandboxDestroyArg],
    ) -> bool | None:
        from .sandbox.destroy import try_destroy_sandbox

        return await try_destroy_sandbox(self, id, **_options(arg, options))

    async def destroy_sandbox(
        self,
        id: str,
        arg: SandboxDestroyArg | None = None,
        **options: Unpack[SandboxDestroyArg],
    ) -> None:
        from .sandbox.destroy import destroy_sandbox

        return await destroy_sandbox(self, id, **_options(arg, options))


def _options(arg, options):
    return {**(arg or {}), **options}


async def check_response(response: Response) -> None:
    if not 200 <= response.status < 300:
        raise Error.from_data(cast(ErrorData, await response.json()))


def wire_arg(arg: Mapping[str, Any]) -> dict[str, Any]:
    from ..location import Arg, Location

    arg = dict(arg)
    for key in ("location", "cache_location"):
        value = arg.get(key)
        if isinstance(value, dict):
            arg[key] = (
                Arg.to_data_string(value)
                if "components" in value
                else Location.to_data_string(value)
            )
    return arg


def response_locations(output: object) -> object:
    from ..location import Location

    if isinstance(output, dict) and isinstance(output.get("location"), str):
        output = {**output, "location": Location.from_data_string(output["location"])}
    return output


client = Client()


def is_retryable_error(error: Exception) -> bool:
    if isinstance(error, TransportError):
        return error.retryable
    return (
        isinstance(
            error,
            (BrokenPipeError, ConnectionAbortedError, ConnectionResetError, EOFError),
        )
        or isinstance(error, OSError)
        and error.errno
        in (errno.EPIPE, errno.ECONNABORTED, errno.ECONNRESET, errno.ENOTCONN)
    )
