"""The default host backed by Python's operating system facilities."""

from __future__ import annotations

import asyncio
import errno
import inspect
import os
import platform
import signal as _signal
from collections.abc import AsyncIterator, Awaitable, Generator, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Self, TypedDict

from .. import _native
from .. import http2 as http2

if TYPE_CHECKING:
    from ..object import ObjectData
    from ..value import ValueData
    from . import MagicOutput


class Outcome(TypedDict):
    exit: int


class TtySize(TypedDict):
    cols: int
    rows: int


architecture = {"arm64": "aarch64", "AMD64": "x86_64"}.get(
    platform.machine(), platform.machine()
)
current = f"{architecture}-{platform.system().lower()}"


def checksum(input: str | bytes, algorithm: str = "sha256") -> str:
    return _native.checksum(
        input.encode() if isinstance(input, str) else input, algorithm
    )


def object_id(data: ObjectData) -> str:
    import json

    return _native.object_id(json.dumps(data, allow_nan=False))


@dataclass
class Child:
    process: asyncio.subprocess.Process

    @property
    def pid(self) -> int:
        return self.process.pid

    @property
    def stdin(self) -> asyncio.StreamWriter | None:
        return self.process.stdin

    @property
    def stdout(self) -> asyncio.StreamReader | None:
        return self.process.stdout

    @property
    def stderr(self) -> asyncio.StreamReader | None:
        return self.process.stderr

    async def wait(self, stopper: Stopper | None = None) -> Outcome:
        if stopper is None:
            status = await self.process.wait()
        else:
            stopped = asyncio.create_task(stopper.event.wait())
            operation = asyncio.create_task(self.process.wait())
            try:
                done, _ = await asyncio.wait(
                    (operation, stopped), return_when=asyncio.FIRST_COMPLETED
                )
                if stopped in done and self.process.returncode is None:
                    self.process.kill()
                status = await operation
            finally:
                stopped.cancel()
                await asyncio.gather(stopped, return_exceptions=True)
        return {"exit": status if status >= 0 else 128 - status}

    def signal(self, signal: int) -> None:
        self.process.send_signal(signal)


async def spawn(
    executable: str | dict[str, Any],
    args: Sequence[str] = (),
    *,
    cwd: str | None = None,
    env: Mapping[str, str] | None = None,
    stdin="inherit",
    stdout="inherit",
    stderr="inherit",
) -> Child:
    if isinstance(executable, dict):
        arg = executable
        executable = arg["executable"]
        args = arg.get("args", ())
        cwd, env = arg.get("cwd"), arg.get("env")
        stdin, stdout, stderr = (
            arg.get(name, "inherit") for name in ("stdin", "stdout", "stderr")
        )

    def stdio(value):
        if value == "inherit":
            return None
        if value == "null":
            return asyncio.subprocess.DEVNULL
        if value == "pipe":
            return asyncio.subprocess.PIPE
        if isinstance(value, int):
            return value
        raise ValueError("invalid host stdio")

    executable = _resolve_executable(executable, os.environ if env is None else env)
    process = await asyncio.create_subprocess_exec(
        executable,
        *args,
        cwd=cwd,
        env=env,
        stdin=stdio(stdin),
        stdout=stdio(stdout),
        stderr=stdio(stderr),
    )
    child = Child(process)
    _children[child.pid] = child
    return child


async def read(
    stream: asyncio.StreamReader | int,
    size: int | None = None,
    stopper: Stopper | None = None,
) -> bytes | None:
    size = 65536 if size is None else size

    async def read_inner():
        if isinstance(stream, int):
            await _ready(stream)
            return os.read(stream, size) or None
        return await stream.read(size) or None

    return await _with_stopper(read_inner(), stopper)


async def write(stream: asyncio.StreamWriter | int, bytes_: bytes) -> None:
    if isinstance(stream, int):
        view = memoryview(bytes_)
        while view:
            await _ready(stream, writing=True)
            written = os.write(stream, view[:4096])
            if not written:
                raise BrokenPipeError("failed to write to the file descriptor")
            view = view[written:]
    else:
        stream.write(bytes_)
        await stream.drain()


async def close(stream: asyncio.StreamReader | asyncio.StreamWriter | int) -> None:
    if isinstance(stream, int):
        os.close(stream)
    elif isinstance(stream, asyncio.StreamReader):
        transport = getattr(stream, "_transport", None)
        if transport is not None:
            transport.close()
    else:
        stream.close()
        await stream.wait_closed()


async def getxattr(path: str, name: str) -> bytes | None:
    if platform.system() == "Darwin":
        if name not in await listxattr(path):
            return None
        process = await asyncio.create_subprocess_exec(
            "xattr",
            "-px",
            name,
            path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode:
            raise OSError(stderr.decode().strip())
        return bytes.fromhex(stdout.decode())
    try:
        return await asyncio.to_thread(getattr(os, "getxattr"), path, name)
    except OSError as error:
        if error.errno in (errno.ENODATA, getattr(errno, "ENOATTR", errno.ENODATA)):
            return None
        raise


async def listxattr(path: str) -> list[str]:
    if platform.system() == "Darwin":
        process = await asyncio.create_subprocess_exec(
            "xattr",
            path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode:
            raise OSError(stderr.decode().strip())
        return stdout.decode().splitlines()
    return await asyncio.to_thread(getattr(os, "listxattr"), path)


def get_tty_size() -> TtySize | None:
    for fd in (1, 2):
        try:
            size = os.get_terminal_size(fd)
            return {"cols": size.columns, "rows": size.lines}
        except OSError:
            continue
    return None


async def exec(
    executable: str | dict[str, Any],
    args: Sequence[str] = (),
    *,
    cwd: str | None = None,
    env: Mapping[str, str] | None = None,
    stdin="inherit",
    stdout="inherit",
    stderr="inherit",
):
    if isinstance(executable, dict):
        arg = executable
        executable = arg["executable"]
        args = arg.get("args", ())
        cwd, env = arg.get("cwd"), arg.get("env")
        stdin, stdout, stderr = (
            arg.get(name, "inherit") for name in ("stdin", "stdout", "stderr")
        )
    executable = _resolve_executable(executable, os.environ if env is None else env)
    for name, value in (("stdin", stdin), ("stdout", stdout), ("stderr", stderr)):
        if value not in ("inherit", "null"):
            raise ValueError(f"{name} must be inherit or null for an exec")
    for fd, value in enumerate((stdin, stdout, stderr)):
        if value == "null":
            null = os.open(os.devnull, os.O_RDONLY if fd == 0 else os.O_WRONLY)
            try:
                os.dup2(null, fd)
            finally:
                if null != fd:
                    os.close(null)
    if cwd is not None:
        os.chdir(cwd)
    os.execvpe(executable, [executable, *args], os.environ if env is None else env)


def kill(pid: int, signal_: int = _signal.SIGTERM) -> None:
    os.kill(pid, signal_)


class Stopper:
    def __init__(self) -> None:
        self.event = asyncio.Event()

    def __await__(self) -> Generator[Any, None, Stopper]:
        async def opened():
            return self

        return opened().__await__()

    def stop(self) -> None:
        self.event.set()

    async def close(self) -> None:
        self.stop()


async def _with_stopper[T](
    awaitable: Awaitable[T], stopper: Stopper | None = None
) -> T:
    if stopper is None:
        return await awaitable
    operation = asyncio.ensure_future(awaitable)
    stopped = asyncio.create_task(stopper.event.wait())
    try:
        done, _ = await asyncio.wait(
            (operation, stopped), return_when=asyncio.FIRST_COMPLETED
        )
        if operation in done:
            return operation.result()
        raise RuntimeError("the operation was stopped")
    finally:
        for task in (operation, stopped):
            if not task.done():
                task.cancel()
        await asyncio.gather(operation, stopped, return_exceptions=True)


async def _ready(fd: int, *, writing: bool = False) -> None:
    loop = asyncio.get_running_loop()
    ready = loop.create_future()

    def notify():
        if not ready.done():
            ready.set_result(None)

    add = loop.add_writer if writing else loop.add_reader
    remove = loop.remove_writer if writing else loop.remove_reader
    try:
        add(fd, notify)
    except (PermissionError, OSError):
        # Regular files do not support readiness notifications on all hosts.
        return
    try:
        await ready
    finally:
        remove(fd)


async def exists(path: str) -> bool:
    return await asyncio.to_thread(os.path.exists, path)


async def remove(path: str) -> None:
    import shutil

    def remove_inner():
        if os.path.isdir(path) and not os.path.islink(path):
            shutil.rmtree(path)
        else:
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass

    await asyncio.to_thread(remove_inner)


async def mkdtemp() -> str:
    import tempfile

    return os.fsdecode(await asyncio.to_thread(tempfile.mkdtemp, prefix="tangram-"))


parallelism = os.cpu_count() or 1


def is_tty(fd: int) -> bool:
    return os.isatty(fd)


def is_foreground_controlling_tty(fd: int) -> bool:
    try:
        return os.isatty(fd) and os.tcgetpgrp(fd) == os.getpgrp()
    except OSError:
        return False


_raw_modes = {}


async def enable_raw_mode(fd: int = 0) -> None:
    import termios
    import tty

    if is_foreground_controlling_tty(fd) and fd not in _raw_modes:
        _raw_modes[fd] = termios.tcgetattr(fd)
        tty.setraw(fd)


async def disable_raw_mode(fd: int = 0) -> None:
    import termios

    previous = _raw_modes.pop(fd, None)
    if previous is not None:
        termios.tcsetattr(fd, termios.TCSADRAIN, previous)


async def sleep(duration: float, stopper: Stopper | None = None) -> None:
    await _with_stopper(asyncio.sleep(duration), stopper)


def write_sync(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        if not written:
            raise BrokenPipeError("failed to write to the file descriptor")
        view = view[written:]


_signal_listeners = {}
_signal_handlers = {}


class SignalListener:
    def __init__(self, name: Literal["sigwinch"]) -> None:
        if name != "sigwinch":
            raise ValueError(f"unsupported signal {name}")
        self.signal = _signal.SIGWINCH
        self.queue: asyncio.Queue[bool] = asyncio.Queue()
        self.closed = asyncio.Event()
        listeners = _signal_listeners.setdefault(self.signal, set())
        if not listeners:
            _signal_handlers[self.signal] = _signal.getsignal(self.signal)

            def receive(signum, frame):
                for listener in tuple(_signal_listeners[signum]):
                    listener.queue.put_nowait(True)
                previous = _signal_handlers[signum]
                if callable(previous):
                    previous(signum, frame)

            _signal.signal(self.signal, receive)
        listeners.add(self)

    def __aiter__(self) -> AsyncIterator[None]:
        return self

    async def __anext__(self) -> None:
        if not self.queue.empty():
            self.queue.get_nowait()
            return None
        if self.closed.is_set():
            raise StopAsyncIteration
        received = asyncio.create_task(self.queue.get())
        closed = asyncio.create_task(self.closed.wait())
        try:
            done, _ = await asyncio.wait(
                (received, closed), return_when=asyncio.FIRST_COMPLETED
            )
            if received in done:
                return None
            raise StopAsyncIteration
        finally:
            for task in (received, closed):
                if not task.done():
                    task.cancel()
            await asyncio.gather(received, closed, return_exceptions=True)

    async def close(self) -> None:
        if self.closed.is_set():
            return
        self.closed.set()
        listeners = _signal_listeners[self.signal]
        listeners.remove(self)
        if not listeners:
            _signal.signal(self.signal, _signal_handlers.pop(self.signal))
            del _signal_listeners[self.signal]

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()


listen_signal = SignalListener
stopper_open = Stopper


async def stopper_stop(stopper: Stopper) -> None:
    stopper.stop()


async def stopper_close(stopper: Stopper) -> None:
    await stopper.close()


_children: dict[int, Child] = {}


async def wait(child: Child | int, stopper: Stopper | None = None) -> Outcome:
    if isinstance(child, int):
        if child not in _children:
            raise ValueError(f"failed to find the process {child}")
        child = _children[child]
    _children.pop(child.pid, None)
    return await child.wait(stopper)


async def signal(pid: int, number: str | int) -> None:
    if isinstance(number, str):
        number = getattr(_signal, f"SIG{number}")
    os.kill(pid, number)


def parse_value(value: str) -> ValueData:
    import json

    return json.loads(_native.parse_value(value))


def stringify_value(value: ValueData) -> str:
    import json

    return _native.stringify_value(
        json.dumps(value, ensure_ascii=False, allow_nan=False)
    )


def magic(value: Any) -> MagicOutput:
    from ..module import Module

    # Stop at the passed Python function; only traverse non-function callable wrappers.
    function = inspect.unwrap(value, stop=inspect.isfunction)
    if not callable(value) or not inspect.isfunction(function):
        raise TypeError("expected a python function")
    namespace = function.__globals__
    module = namespace.get("__tangram_module__")
    if not isinstance(module, Module) or module.kind not in ("js", "py", "ts"):
        raise ValueError("failed to find the Tangram module for the function")
    name = function.__name__
    if namespace.get(name) is not value:
        names = sorted(name for name, export in namespace.items() if export is value)
        if not names:
            raise ValueError("failed to find an export for the function")
        name = names[0]
    return {"module": module.to_data(), "export": name}


async def read_file(path: str) -> bytes:
    from pathlib import Path

    return await asyncio.to_thread(Path(path).read_bytes)


def _resolve_executable(executable: str, env: Mapping[str, str]) -> str:
    if os.path.isabs(executable) or os.sep in executable:
        return executable
    path = env.get("PATH")
    if path is not None:
        for directory in path.split(os.pathsep):
            candidate = os.path.join(directory, executable)
            if os.path.isfile(candidate):
                return candidate
    raise FileNotFoundError(f"failed to find {executable} in PATH")
