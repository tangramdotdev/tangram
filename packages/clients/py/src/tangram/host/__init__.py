"""The active Tangram host and host replacement interface."""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Mapping
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    Never,
    NotRequired,
    Protocol,
    TypedDict,
)

from ..http import Request, Response
from . import default

if TYPE_CHECKING:
    from ..module import ModuleDataObject
    from ..object import ObjectWireData
    from ..value import ValueData

Signal = Literal["sigwinch"]
Stdio = Literal["inherit", "null", "pipe"]


class MagicOutput(TypedDict):
    module: ModuleDataObject
    export: NotRequired[str | None]


class SpawnArg(TypedDict):
    args: list[str]
    cwd: str | None
    env: dict[str, str]
    executable: str
    stderr: Stdio
    stdin: Stdio
    stdout: Stdio


class SpawnOutput(TypedDict):
    pid: int
    stderr: asyncio.StreamReader | int | None
    stdin: asyncio.StreamWriter | int | None
    stdout: asyncio.StreamReader | int | None


Outcome = default.Outcome


class Http2Session(Protocol):
    closed: bool

    async def send(self, request: Request) -> Response: ...
    async def close(self) -> None: ...


class Http2SessionType(Protocol):
    async def connect(self, authority: str) -> Http2Session: ...


class Http2(Protocol):
    Session: Http2SessionType


class SignalListenerProtocol(Protocol):
    def __aiter__(self) -> AsyncIterator[None]: ...
    async def close(self) -> None: ...


class Host(Protocol):
    MagicOutput: ClassVar = MagicOutput
    Http2: ClassVar = Http2
    Signal: ClassVar = Signal
    Stdio: ClassVar = Stdio
    SpawnArg: ClassVar = SpawnArg
    SpawnOutput: ClassVar = SpawnOutput
    Outcome: ClassVar = Outcome
    Stopper: ClassVar = default.Stopper
    SignalListener: ClassVar = SignalListenerProtocol

    http2: Http2
    current: str
    parallelism: int

    def checksum(self, input: str | bytes, algorithm: str) -> str: ...
    async def close(
        self, fd: asyncio.StreamReader | asyncio.StreamWriter | int
    ) -> None: ...
    async def disable_raw_mode(self, fd: int) -> None: ...
    async def enable_raw_mode(self, fd: int) -> None: ...
    async def exec(self, arg: SpawnArg) -> Never: ...
    async def exists(self, path: str) -> bool: ...
    def get_tty_size(self) -> dict[str, int] | None: ...
    async def getxattr(self, path: str, name: str) -> bytes | None: ...
    async def listxattr(self, path: str) -> list[str]: ...
    def is_foreground_controlling_tty(self, fd: int) -> bool: ...
    def is_tty(self, fd: int) -> bool: ...
    def listen_signal(self, signal: Signal) -> SignalListenerProtocol: ...
    def magic(self, value: Any) -> MagicOutput: ...
    async def mkdtemp(self) -> str: ...
    def object_id(self, object: "ObjectWireData") -> str: ...
    def parse_value(self, value: str) -> "ValueData": ...
    async def read(
        self,
        fd: asyncio.StreamReader | int,
        length: int | None = None,
        stopper: default.Stopper | None = None,
    ) -> bytes | None: ...
    async def read_file(self, path: str) -> bytes: ...
    async def remove(self, path: str) -> None: ...
    async def signal(self, pid: int, signal: str | int) -> None: ...
    async def sleep(
        self, duration: float, stopper: default.Stopper | None = None
    ) -> None: ...
    def stringify_value(self, value: "ValueData") -> str: ...
    async def spawn(self, arg: SpawnArg) -> default.Child: ...
    async def stopper_close(self, stopper: default.Stopper) -> None: ...
    def stopper_open(self) -> Awaitable[default.Stopper]: ...
    async def stopper_stop(self, stopper: default.Stopper) -> None: ...
    async def wait(
        self, pid: int, stopper: default.Stopper | None = None
    ) -> Outcome: ...
    async def write(self, fd: asyncio.StreamWriter | int, bytes: bytes) -> None: ...
    def write_sync(self, fd: int, bytes: bytes) -> None: ...


Child = default.Child
SignalListener = default.SignalListener
Stopper = default.Stopper
checksum = default.checksum
close = default.close
current = default.current
disable_raw_mode = default.disable_raw_mode
enable_raw_mode = default.enable_raw_mode
exec = default.exec
exists = default.exists
get_tty_size = default.get_tty_size
getxattr = default.getxattr
http2 = default.http2
is_foreground_controlling_tty = default.is_foreground_controlling_tty
is_tty = default.is_tty
kill = default.kill
listen_signal = default.listen_signal
listxattr = default.listxattr
magic = default.magic
mkdtemp = default.mkdtemp
object_id = default.object_id
parallelism = default.parallelism
parse_value = default.parse_value
read = default.read
read_file = default.read_file
remove = default.remove
signal = default.signal
sleep = default.sleep
spawn = default.spawn
stopper_close = default.stopper_close
stopper_open = default.stopper_open
stopper_stop = default.stopper_stop
stringify_value = default.stringify_value
wait = default.wait
write = default.write
write_sync = default.write_sync


def set_host(other: object) -> None:
    """Replace host operations while retaining the public host module identity."""
    import sys

    target = sys.modules[__name__]
    items = (
        other.items()
        if isinstance(other, Mapping)
        else (
            (name, getattr(other, name))
            for name in dir(other)
            if not name.startswith("_")
        )
    )
    for name, value in items:
        if not name.startswith("_") and name != "set_host":
            setattr(target, name, value)


set_host(default)
