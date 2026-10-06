"""Create sandboxed and local processes."""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Callable
from typing import TYPE_CHECKING, Literal, cast, overload

from .. import host
from ..async_property import async_property
from ..authorization import inherit
from ..client import last_output
from ..command import (
    Command,
    CommandArgObject,
    CommandArgument,
    CommandInput,
    command_value,
)
from ..directory import Directory
from ..error import Error
from ..file import File
from ..file.xattrs import read_error, read_outcome, read_output
from ..object import Object
from ..referent import Referent, ReferentOptions
from ..resolve import Unresolved, resolve
from ..symlink import Symlink
from ..value import Placeholder, Template, Value

if TYPE_CHECKING:
    from ..client import Client
    from ..value import ValueType

from . import ArgObject, Builder, Process, process_arg_resolved


async def _spawn_arg_from_resolved_with_sandbox(arg, sandbox, client):
    options: ReferentOptions = {}
    commands: list[CommandInput] = []
    command = arg.get("command")
    if isinstance(command, Referent):
        options.update(command.options or {})
        command = command.node
    if command is not None:
        commands.append(command)
        if isinstance(command, Command):
            options["tokens"] = inherit(
                options.get("tokens") or {}, command.state.tokens
            )
    if "name" in arg:
        options["name"] = arg["name"]
    from . import env

    if (
        sandbox is not None
        and arg.get("executable") is not None
        and arg["executable"] == env.get("SHELL")
    ):
        arg = {**arg, "executable": "sh"}
    command_arg = {
        key: arg[key]
        for key in ("args", "cwd", "env", "executable", "host", "user")
        if key in arg
    }
    stdin = arg.get("stdin")
    process_stdin = "inherit"
    if isinstance(stdin, str) and stdin in ("inherit", "log", "null", "pipe", "tty"):
        process_stdin = stdin
    elif "stdin" in arg:
        command_arg["stdin"] = stdin
    commands.append(cast(CommandArgObject, command_arg))
    from ..blob import Blob
    from ..command import CommandExecutable

    command = await Command.arg(*commands, client=client)
    executable = command.get("executable")
    if isinstance(executable, (Directory, File, Symlink)):
        executable = {"artifact": executable, "path": None}
    elif isinstance(executable, str):
        executable = {"artifact": None, "path": executable}
    elif executable is not None:
        executable = {
            "artifact": executable.get("artifact"),
            "path": executable.get("path"),
        }
    else:
        raise ValueError("cannot create a command without an executable")
    args = command.get("args") or []
    env = command.get("env") or {}
    stdin = command.get("stdin")
    stdin = None if stdin is None else await Blob.new(stdin, client=client)
    objects = [
        *[child for value in args for child in Command.Value.children(value)],
        *[child for value in env.values() for child in Command.Value.children(value)],
        *CommandExecutable.children(executable),
        *([] if stdin is None else [stdin]),
    ]
    await Value.store(objects, client)
    artifact = executable.get("artifact")
    data = {
        "args": [value.to_data() for value in args],
        "env": {name: value.to_data() for name, value in env.items()},
        "executable": Referent(
            CommandExecutable.to_data(executable),
            artifact.to_referent().options if artifact is not None else {},
        ).to_data(),
    }
    for key in ("cwd", "host", "user"):
        if command.get(key) is not None:
            data[key] = command[key]
    if stdin is not None:
        data["stdin"] = stdin.to_referent().to_data()
    # The spawn endpoint accepts an inline command, preserving executable proofs.
    state = {
        "command": Referent(data, options),
        "public": False,
        "retry": False,
        "stderr": arg.get("stderr") or "inherit",
        "stdin": process_stdin,
        "stdout": arg.get("stdout") or "inherit",
    }
    for key in ("cached", "checksum", "cache_location", "location", "tty"):
        if key in arg and (key != "cached" or arg[key] is not None):
            state[key] = arg[key]
    if arg.get("debug") is not None and arg["debug"] is not False:
        state["debug"] = {} if arg["debug"] is True else arg["debug"]
    if sandbox is not None:
        state["sandbox"] = sandbox
    return command, state


async def spawn_resolved(builder, resolved):
    from . import cwd, env

    client = builder.client
    arg = await process_arg_resolved(*resolved, client=client)
    if builder._validate is not None:
        result = builder._validate(arg)
        if result is not None:
            await resolve(result)

    sandbox = normalize_sandbox(arg)
    if sandbox is None:
        resolved.insert(0, {"cwd": cwd, "env": dict(env)})
        arg = await process_arg_resolved(*resolved, client=client)
    _, state = await _spawn_arg_from_resolved_with_sandbox(arg, sandbox, client)
    if sandbox is None:
        return await spawn_unsandboxed(state, state["command"].options, client=client)
    return await spawn_sandboxed(
        state,
        state["command"].options,
        mode=builder._connection if builder.operation == "spawn" else "run",
        client=client,
    )


async def spawn_sandboxed(
    arg, options=None, mode="spawn", *, client: Client | None = None
):
    from ..client import client as default_client
    from ..location import Arg as LocationArg
    from . import env, stdio
    from .connect import Connection
    from .outcome import Outcome

    client = client or default_client
    state = dict(arg)
    no_tty = state.get("tty") is False
    provide = {
        name: state.get(name) in ("pipe", "tty")
        for name in ("stdin", "stdout", "stderr")
    }
    foreground = {
        name: host.is_foreground_controlling_tty(fd)
        for name, fd in (("stdin", 0), ("stdout", 1), ("stderr", 2))
    }

    def resolve_inherited(mode, foreground, background):
        original = mode if mode is not None else "inherit"
        if original != "inherit":
            return None, original
        spawn = "tty" if not no_tty and foreground else background
        return (None if spawn == "null" else spawn), spawn

    local = {}
    for name in ("stdin", "stdout", "stderr"):
        background = "null" if name == "stdin" and host.is_tty(0) else "pipe"
        local[name], state[name] = resolve_inherited(
            state.get(name), foreground[name], background
        )
    tty_arg = arg.get("tty")
    tty = None
    if tty_arg is True:
        size = host.get_tty_size()
        if size is not None:
            tty = {"size": size}
    elif tty_arg is not None and tty_arg is not False:
        tty = tty_arg
    has_tty_stream = any(state[name] == "tty" for name in ("stdin", "stdout", "stderr"))
    if tty is None and has_tty_stream:
        size = host.get_tty_size()
        if size is not None:
            tty = {"size": size}
    local_tty = tty is not None and any(foreground.values())
    if tty is not None and has_tty_stream:
        referent = state["command"]
        if not isinstance(referent, Referent):
            referent = Referent.from_data(referent)
        if isinstance(referent.node, str):
            command = Command.with_referent(referent)
            data = await command.object(client)
            command_env = dict(data["env"])
            changed = False
            for name in ("COLORTERM", "TERM"):
                if name in env and name not in command_env:
                    command_env[name] = command_value(env[name])
                    changed = True
            if changed:
                command = Command.with_object({**data, "env": command_env})
                command_id = await command.store(client)
                state["command"] = Referent(
                    command_id,
                    {**(referent.options or {}), "tokens": command.state.tokens},
                )
        else:
            command_env = dict(referent.node.get("env", {}))
            changed = False
            for name in ("COLORTERM", "TERM"):
                if name in env and name not in command_env:
                    command_env[name] = command_value(env[name]).to_data()
                    changed = True
            if changed:
                state["command"] = Referent(
                    {**referent.node, "env": command_env}, referent.options
                )
    state["retry"] = state.get("retry") or False
    if tty is not None:
        state["tty"] = tty
    else:
        state.pop("tty", None)
    reads = []
    if mode == "run":
        streams = [name for name in ("stdout", "stderr") if local[name] is not None]
        if streams:
            reads.append({"streams": streams})
        for name in ("stdout", "stderr"):
            if provide[name]:
                reads.append({"streams": [name]})
    spawn_location = state.pop("location", None)
    opened = await Connection.open(
        state, client=client, mode=mode, reads=reads, location=spawn_location
    )
    output = opened.initial.output
    connection = opened if mode == "run" else None
    if not isinstance(output["process"], str):
        raise ValueError("expected a sandboxed process id")
    location = output.get("location")
    if location is not None:
        location = LocationArg.from_location(location)
    outcome = output.get("outcome")
    process = Process(
        output["process"],
        client=client,
        connection=connection,
        location=location,
        options=options or {},
        lease=output.get("lease"),
        tokens=output.get("tokens") or {},
        outcome=Outcome.from_data(outcome) if outcome is not None else None,
    )
    process.spawn_output = output
    process.stdin = stdio.Writer(process, "stdin", unavailable=not provide["stdin"])
    process.stdout = stdio.Reader(process, "stdout", unavailable=not provide["stdout"])
    process.stderr = stdio.Reader(process, "stderr", unavailable=not provide["stderr"])
    if any(value is not None for value in local.values()) or local_tty:
        process._stdio_promise = asyncio.create_task(
            stdio.task(
                process.id,
                location,
                process.tokens,
                local["stdin"],
                local["stdout"],
                local["stderr"],
                local_tty,
                connection,
                client=client,
            )
        )
    return process


@overload
def builder[O: ValueType](
    function_: Callable[..., Unresolved[O]], *args: CommandArgument, **options
) -> Builder[Literal["spawn"], O]: ...


@overload
def builder[A, O: ValueType](
    command: Command[A, O], *args, **options
) -> Builder[Literal["spawn"], O]: ...


@overload
def builder(*args, **options) -> Builder[Literal["spawn"], ValueType]: ...


def builder(*args, **options):
    if args and callable(args[0]) and not hasattr(args[0], "__await__"):
        from ..resolve import capture

        function_ = args[0]

        async def command():
            return {
                "command": await Command.py_arg(function_, function_args, client=client)
            }

        client = options.get("client")
        builder = Builder("spawn", command(), **options)
        function_args = capture(args[1:], builder._memo)
        return builder
    if args and isinstance(args[0], list) and hasattr(args[0], "raw"):
        from ..assert_ import assert_
        from ..template import raw
        from . import env

        strings, *placeholders = args
        template = raw(strings, *placeholders)
        executable = env.get("SHELL")
        if executable is None:
            executable = "sh"
        assert_(Command.Arg.Executable.is_(executable))
        shell_arg: ArgObject = {"executable": executable, "args": ["-c", template]}
        args = (shell_arg,)
    return Builder("spawn", *args, **options)


spawn = builder


class LocalProcess(Process):
    def __init__(self, child, command, client, temp_path, stopper=None):
        self.child, self._command, self.client = child, command, client
        from .stdio import Reader, Writer

        self.id = child.pid
        self.connection = None
        self.options = {}
        self._wait_result = None
        self._wait_promise = None
        self._stdio_promise = None
        self._stopper = stopper
        self._owned = False
        self.tokens = {}
        self.location = None
        self.lease = None
        self.stdin = Writer(fd=child.stdin, unavailable=child.stdin is None)
        self.stdout = Reader(fd=child.stdout, unavailable=child.stdout is None)
        self.stderr = Reader(
            stream="stderr", fd=child.stderr, unavailable=child.stderr is None
        )
        self.temp_path = temp_path
        self.pid = child.pid
        self._wait = asyncio.create_task(self._wait_inner())

    @async_property
    async def command(self):
        return self._command

    async def _command_field(self, field, default=None):
        return (await self._command.load(self.client)).get(field, default)

    async def read(self, stream="stdout", **options):
        reader = getattr(self.child, stream)
        if reader is None:
            raise ValueError("the process stream is not a pipe")
        while chunk := await reader.read(options.get("size", 32768)):
            yield chunk

    async def write(self, bytes_):
        if self.child.stdin is None:
            raise ValueError("the process stdin is not a pipe")
        await host.write(self.child.stdin, bytes_)
        return len(bytes_)

    async def end(self):
        if self.child.stdin is not None:
            await host.close(self.child.stdin)

    async def wait(self):
        outcome = await asyncio.shield(self._wait)
        from .outcome import Outcome

        Outcome.inherit_tokens(outcome, self.tokens)
        return outcome

    async def _wait_inner(self):
        return await wait_unsandboxed(
            self.child.pid,
            {"stdin": self.stdin, "stdout": self.stdout, "stderr": self.stderr},
            self._stopper,
            self.temp_path,
            os.path.join(self.temp_path, "output"),
            client=self.client,
        )

    async def signal(self, signal):
        self.child.signal(signal)

    async def cancel(self):
        self.child.signal(15)

    async def detach(self):
        return None

    async def close(self):
        if not self._wait.done():
            await self.cancel()
        await self.wait()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()


async def prepare_local(command, client, output_path, options=None):
    data = await command.load(client)
    if data.get("stdin") is not None or data.get("user") is not None:
        if data.get("stdin") is not None:
            raise ValueError(
                "command stdin blobs are not supported for unsandboxed processes"
            )
        raise ValueError("setting a user is not supported for unsandboxed processes")
    artifacts = {
        child.id: child
        for child in await command.children(client)
        if isinstance(child, (Directory, File, Symlink))
    }
    paths = {}
    options = options or {}
    if artifacts:
        for child in artifacts.values():
            await child.store(client)
        referents = []
        for child in artifacts.values():
            referent = child.to_referent()
            inherit(
                referent.options.setdefault("tokens", {}),
                options.get("tokens", {}),
                child.id,
            )
            if referent.options.get("location") is None and "location" in options:
                referent.options["location"] = options["location"]
            referents.append(referent)
        result = await last_output(
            await client.checkout(referents, dependencies=True, force=False)
        )
        if result is None:
            raise ValueError("stream ended without output")
        if len(result["paths"]) != len(artifacts):
            raise ValueError("checkout returned an unexpected number of paths")
        paths = dict(zip(artifacts, result["paths"], strict=True))

    def render(value):
        if isinstance(value, str):
            return value
        if isinstance(value, (Directory, File, Symlink)):
            return paths[value.id]
        if isinstance(value, Template):
            return "".join(render(component) for component in value.components)
        if isinstance(value, Placeholder):
            if value.name == "output":
                return output_path
            raise ValueError("invalid placeholder")
        return Value.stringify(value)

    def render_command_value(value):
        value = command_value(value)
        return (
            render(value.value)
            if value.kind == "string"
            else Value.stringify(value.value)
        )

    executable = data["executable"]
    path = executable.get("path")
    if executable.get("artifact") is not None:
        root = paths[executable["artifact"].id]
        path = os.path.join(root, path) if path else root
    if path is None:
        raise ValueError("missing command executable")
    args = [render_command_value(value) for value in data.get("args", [])]
    env = {}
    for name, value in data.get("env", {}).items():
        if name.startswith("TANGRAM_ENV_"):
            raise ValueError("env vars prefixed with TANGRAM_ENV_ are reserved")
        env[name] = render_command_value(value)
        if not (value.kind == "string" and isinstance(value.value, str)):
            env[f"TANGRAM_ENV_{name}"] = Value.stringify(value.value)
    for name in (
        "CONFIG",
        "DIRECTORY",
        "JS_DEBUG",
        "JS_DEBUG_ADDR",
        "JS_DEBUG_MODE",
        "JS_ENGINE",
        "MODE",
        "OUTPUT",
        "TOKEN",
        "TRACING",
        "URL",
    ):
        env.pop(f"TANGRAM_{name}", None)
    env["TANGRAM_OUTPUT"] = output_path
    for name in ("token", "url"):
        value = client.arg().get(name)
        if value is not None:
            env[f"TANGRAM_{name.upper()}"] = value
    from . import env as process_env

    engine = process_env.get("TANGRAM_JS_ENGINE")
    env["TANGRAM_JS_ENGINE"] = engine if isinstance(engine, str) else "auto"
    return path, args, env, data.get("cwd")


async def spawn_arg(*args, client: Client | None = None):
    from ..client import client as default_client

    resolved = await asyncio.gather(*(resolve(arg) for arg in args))
    arg = await process_arg_resolved(*resolved, client=client or default_client)
    return await spawn_arg_from_resolved(arg, client=client)


async def spawn_arg_from_resolved(arg, *, client: Client | None = None):
    from ..client import client as default_client
    from . import cwd, env

    client = client or default_client
    sandbox = normalize_sandbox(arg)
    defaults = [{"cwd": cwd, "env": dict(env)}] if sandbox is None else []
    arg = await process_arg_resolved(*defaults, arg, client=client)
    _, state = await _spawn_arg_from_resolved_with_sandbox(arg, sandbox, client)
    return {"arg": state, "options": state["command"].options}


async def wait_unsandboxed(
    pid, stdio, stopper, temp_path, output_path, *, client: Client | None = None
):
    from ..client import client as default_client
    from .outcome import Outcome

    client = client or default_client
    outcome = None
    error = None
    try:
        outcome = {"error": None, **await host.wait(pid, stopper)}
        if await host.exists(output_path):
            outcome_bytes = await read_outcome(output_path)
            if outcome_bytes is not None:
                value = Outcome.from_data(json.loads(outcome_bytes.decode()))
                outcome["error"] = value["error"]
                if "output" in value:
                    outcome["output"] = value["output"]
            else:
                output_bytes = await read_output(output_path)
                if output_bytes is not None:
                    outcome["output"] = Value.parse(output_bytes.decode())
                error_bytes = await read_error(output_path)
                if error_bytes is not None:
                    string = error_bytes.decode()
                    try:
                        value = json.loads(string)
                        outcome["error"] = (
                            Error.with_id(value)
                            if isinstance(value, str)
                            else Error.from_data(value)
                        )
                    except ValueError:
                        outcome["error"] = Error.with_referent(
                            Referent.from_data_string(string)
                        )
            if (
                outcome_bytes is None
                and outcome["error"] is None
                and "output" not in outcome
            ):
                output = await last_output(
                    await client.checkin(
                        output_path,
                        options={
                            "checkout_pointers": True,
                            "destructive": True,
                            "deterministic": True,
                            "ignore": False,
                            "local_dependencies": True,
                            "locked": True,
                            "root": True,
                            "solve": True,
                            "unsolved_dependencies": False,
                            "watch": False,
                        },
                    )
                )
                if output is None:
                    raise ValueError("stream ended without output")
                outcome["output"] = Object.with_referent(output["artifact"])
    except BaseException as exception:
        error = exception
    try:
        if stopper is not None:
            await host.stopper_close(stopper)
    except BaseException as exception:
        if error is None:
            error = exception
    try:
        for name in ("stdin", "stdout", "stderr"):
            await stdio[name].close()
        await host.remove(temp_path)
    except BaseException as exception:
        if error is None:
            error = exception
    if error is not None:
        raise error
    assert outcome is not None
    return outcome


async def prepare_unsandboxed_command(
    arg, output_path=None, *, client: Client | None = None
):
    from ..client import client as default_client

    client = client or default_client
    if "tty" in arg:
        raise ValueError("tty is not supported for unsandboxed processes")
    if "sandbox" in arg:
        raise ValueError("sandboxing is not supported for unsandboxed processes")
    if str(arg.get("stdin") or "inherit").startswith("blb_"):
        raise ValueError("blob stdin is not supported for unsandboxed processes")
    referent = arg["command"]
    if not isinstance(referent, Referent):
        referent = Referent.from_data(referent)
    if isinstance(referent.node, str):
        command = Command.with_referent(referent)
    else:
        data = dict(referent.node)
        executable = data["executable"]
        if not isinstance(executable, Referent):
            executable = Referent.from_data(executable)
        data["executable"] = executable.node
        stdin = data.get("stdin")
        if stdin is not None:
            stdin = (
                Referent.from_data_string(stdin)
                if isinstance(stdin, str)
                else Referent.from_data(stdin)
            )
            data["stdin"] = stdin.node
        command = Object.from_data({"kind": "command", "value": data})
        loaded = await command.load(client)
        artifact = loaded["executable"].get("artifact")
        if artifact is not None:
            loaded["executable"]["artifact"] = Object.with_referent(
                Referent(artifact.id, executable.options)
            )
    temp_path = await host.mkdtemp()
    output_path = output_path or os.path.join(temp_path, "output")
    executable, args, env, cwd = await prepare_local(
        command, client, output_path, referent.options
    )
    debug = arg.get("debug")
    if debug is not None:
        env["TANGRAM_JS_DEBUG"] = "true"
        if debug.get("addr") is not None:
            env["TANGRAM_JS_DEBUG_ADDR"] = debug["addr"]
        if debug.get("mode") not in (None, "normal"):
            env["TANGRAM_JS_DEBUG_MODE"] = debug["mode"]
    return {
        "args": args,
        "cwd": cwd,
        "env": env,
        "executable": executable,
        "temp_path": temp_path,
        "output_path": output_path,
        "command": command,
    }


async def spawn_unsandboxed(arg, options=None, *, client: Client | None = None):
    from ..client import client as default_client

    client = client or default_client
    prepared = await prepare_unsandboxed_command(arg, client=client)
    child = await host.spawn(
        prepared["executable"],
        prepared["args"],
        cwd=prepared["cwd"],
        env=prepared["env"],
        **{
            name: render_stdio(arg.get(name) or "inherit", name)
            for name in ("stdin", "stdout", "stderr")
        },
    )
    stopper = await host.stopper_open()
    process = LocalProcess(
        child, prepared["command"], client, prepared["temp_path"], stopper
    )
    process.options = {**(options or {})}
    return process


def render_stdio(stdio, stream):
    if stdio in ("inherit", "null", "pipe"):
        return stdio
    if stdio in ("log", "tty"):
        raise ValueError(f"{stdio} stdio is not supported for unsandboxed processes")
    raise ValueError(
        f"blob {'stdin' if stream == 'stdin' else 'stdio'} "
        "is not supported for unsandboxed processes"
    )


def is_sandbox_arg(value):
    return isinstance(value, dict)


def is_network_enabled(value=None):
    return value is not None and value is not False


def normalize_sandbox(arg):
    from ..sandbox import Mount, arg_to_data, normalize_network

    fields = {
        key: arg[key]
        for key in ("cpu", "memory", "network", "owner")
        if arg.get(key) is not None
    }
    mounts = arg.get("mounts") or []
    ports = arg.get("ports") or []
    sandbox = arg.get("sandbox")
    if isinstance(sandbox, str):
        if fields or mounts or ports:
            raise ValueError(
                "cpu, memory, mounts, network, owner, and ports "
                "are not supported for existing sandboxes"
            )
        return sandbox
    if sandbox is None or sandbox is False:
        if not fields and not mounts and not ports:
            return None
        sandbox = {}
    if sandbox is True:
        sandbox = {}
    output = arg_to_data({**sandbox, "ttl": sandbox.get("ttl", 0)})
    output.pop("host", None)
    for key in ("cpu", "memory", "owner"):
        if key in fields:
            output[key] = fields[key]
    if mounts:
        output["mounts"] = [
            *(output.get("mounts") or []),
            *(Mount.to_data_string(value) for value in mounts),
        ]
    if "network" in fields or ports:
        network = normalize_network(
            fields.get(
                "network",
                output["network"]
                if output.get("network", {}).get("kind") == "bridge"
                else sandbox.get("network"),
            ),
            ports,
        )
        if network is not None:
            output["network"] = network
    return output
