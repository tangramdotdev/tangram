"""Process class for Tangram."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from tangram.handle import SpawnArg, SpawnOutput, get_handle
from tangram.object_ import Object_


@dataclass
class ProcessWait:
    """Result of waiting for a process."""

    checksum: Optional[str]
    error: Optional[Dict[str, Any]]
    exit: int
    output: Optional[Any]

    @property
    def succeeded(self) -> bool:
        """Check if the process succeeded."""
        return self.exit == 0 and self.error is None


# Module-level process context (like JS tg.process).
process: Dict[str, Any] = {}


def set_process(new_process: Dict[str, Any]) -> None:
    """Set the global process context."""
    global process
    process.clear()
    process.update(new_process)


class Process:
    """A process represents a running or completed execution."""

    # Static property for the current process (like JS Process.current).
    current: Optional["Process"] = None

    def __init__(self, id: str, remote: Optional[str] = None) -> None:
        """Initialize the process."""
        self._id = id
        self._remote = remote
        self._data: Optional[Dict[str, Any]] = None

    @property
    def id(self) -> str:
        """Get the process ID."""
        return self._id

    @property
    def remote(self) -> Optional[str]:
        """Get the remote this process is on."""
        return self._remote

    @classmethod
    async def spawn(
        cls,
        command: Object_,
        *,
        checksum: Optional[str] = None,
        create: bool = True,
        mounts: Optional[List[Dict[str, Any]]] = None,
        network: bool = False,
        parent: Optional[str] = None,
        remote: Optional[str] = None,
        retry: bool = True,
        stderr: Optional[str] = None,
        stdin: Optional[str] = None,
        stdout: Optional[str] = None,
    ) -> Process:
        """Spawn a new process."""
        handle = get_handle()

        # Get command data.
        command_data = await command.load() if hasattr(command, "load") else command

        arg = SpawnArg(
            checksum=checksum,
            command=command_data,
            create=create,
            mounts=mounts or [],
            network=network,
            parent=parent,
            remote=remote,
            retry=retry,
            stderr=stderr,
            stdin=stdin,
            stdout=stdout,
        )

        output: SpawnOutput = await handle.spawn_process(arg)
        return cls(output.process, output.remote)

    async def load(self) -> Dict[str, Any]:
        """Load process data."""
        if self._data is None:
            handle = get_handle()
            self._data = await handle.get_process(self._id, self._remote)
        return self._data

    async def wait(self) -> ProcessWait:
        """Wait for the process to complete."""
        handle = get_handle()
        result = await handle.wait_process(self._id, self._remote)
        return ProcessWait(
            checksum=result.get("checksum"),
            error=result.get("error"),
            exit=result.get("exit", 1),
            output=result.get("output"),
        )

    async def reload(self) -> Dict[str, Any]:
        """Reload the process data (clears cached data first)."""
        self._data = None
        return await self.load()

    async def command(self) -> Object_:
        """Get the process command."""
        data = await self.load()
        command_id = data.get("command")
        if command_id is None:
            raise RuntimeError("process has no command")

        from tangram.command import Command

        return Command.with_id(command_id)

    async def args(self) -> List[Any]:
        """Get the command arguments."""
        cmd = await self.command()
        from tangram.command import Command

        if isinstance(cmd, Command):
            return await cmd.args()
        return []

    async def cwd(self) -> Optional[str]:
        """Get the working directory."""
        cmd = await self.command()
        from tangram.command import Command

        if isinstance(cmd, Command):
            return await cmd.cwd()
        return None

    async def env(self, name: Optional[str] = None) -> Any:
        """Get environment variables. If name provided, return that variable."""
        cmd = await self.command()
        from tangram.command import Command

        if isinstance(cmd, Command):
            env_vars = await cmd.env()
            if name is not None:
                return env_vars.get(name)
            return env_vars
        return {} if name is None else None

    async def executable(self) -> Optional[Any]:
        """Get the executable."""
        cmd = await self.command()
        from tangram.command import Command

        if isinstance(cmd, Command):
            return await cmd.executable()
        return None

    async def mounts(self) -> List[Any]:
        """Get the mounts."""
        data = await self.load()
        return data.get("mounts", [])

    async def network(self) -> bool:
        """Get the network access flag."""
        data = await self.load()
        return data.get("network", False)

    async def user(self) -> Optional[str]:
        """Get the user."""
        cmd = await self.command()
        from tangram.command import Command

        if isinstance(cmd, Command):
            return await cmd.user()
        return None

    async def output(self) -> Any:
        """Wait for the process and get its output."""
        wait = await self.wait()
        if wait.error is not None:
            raise RuntimeError(f"process failed: {wait.error}")
        return wait.output


class ProcessContext:
    """Context for the current process (read from environment variables)."""

    @property
    def id(self) -> str:
        """Get the current process ID."""
        process_id = os.environ.get("TANGRAM_PROCESS")
        if process_id is None:
            raise RuntimeError("not running in a Tangram process")
        return process_id

    @property
    def remote(self) -> Optional[str]:
        """Get the current process remote."""
        return os.environ.get("TANGRAM_REMOTE")

    def current(self) -> Process:
        """Get the current process."""
        return Process(self.id, self.remote)
