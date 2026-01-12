"""Handle interface for runtime operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Protocol,
    Union,
    runtime_checkable,
)

if TYPE_CHECKING:
    from tangram.blob import Blob
    from tangram.object_ import Object_
    from tangram.process import Process


@dataclass
class ReadArg:
    """Arguments for reading a blob."""

    blob: str
    position: Optional[Union[int, str]] = None
    length: Optional[int] = None
    size: Optional[int] = None


@dataclass
class SpawnArg:
    """Arguments for spawning a process."""

    checksum: Optional[str]
    command: Dict[str, Any]
    create: bool
    mounts: List[Dict[str, Any]]
    network: bool
    parent: Optional[str]
    remote: Optional[str]
    retry: bool
    stderr: Optional[str]
    stdin: Optional[str]
    stdout: Optional[str]


@dataclass
class SpawnOutput:
    """Output from spawning a process."""

    process: str
    remote: Optional[str]


@dataclass
class PostObjectBatchArg:
    """Arguments for posting a batch of objects."""

    objects: Dict[str, Any]


@runtime_checkable
class Handle(Protocol):
    """Protocol defining the runtime handle interface."""

    async def read(self, arg: ReadArg) -> bytes:
        """Read bytes from a blob."""
        ...

    async def write(self, bytes_: Union[str, bytes]) -> str:
        """Write bytes to a blob and return its ID."""
        ...

    async def get_object(self, id: str) -> Dict[str, Any]:
        """Get object data by ID."""
        ...

    async def post_object_batch(self, arg: PostObjectBatchArg) -> None:
        """Post a batch of objects."""
        ...

    async def get_process(self, id: str, remote: Optional[str] = None) -> Dict[str, Any]:
        """Get process data by ID."""
        ...

    async def spawn_process(self, arg: SpawnArg) -> SpawnOutput:
        """Spawn a new process."""
        ...

    async def wait_process(self, id: str, remote: Optional[str] = None) -> Dict[str, Any]:
        """Wait for a process to complete."""
        ...

    def checksum(self, input_: Union[str, bytes], algorithm: str) -> str:
        """Compute a checksum."""
        ...

    def object_id(self, data: Dict[str, Any]) -> str:
        """Compute the ID for object data."""
        ...

    def log(self, stream: Literal["stdout", "stderr"], string: str) -> None:
        """Log a message to stdout or stderr."""
        ...

    async def sleep(self, duration: float) -> None:
        """Sleep for a duration in seconds."""
        ...

    # Encoding functions.
    def base64_decode(self, data: str) -> bytes:
        """Decode base64 to bytes."""
        ...

    def base64_encode(self, data: bytes) -> str:
        """Encode bytes to base64."""
        ...

    def hex_decode(self, data: str) -> bytes:
        """Decode hex to bytes."""
        ...

    def hex_encode(self, data: bytes) -> str:
        """Encode bytes to hex."""
        ...

    def json_decode(self, data: str) -> Any:
        """Decode JSON to a value."""
        ...

    def json_encode(self, data: Any) -> str:
        """Encode a value to JSON."""
        ...

    def toml_decode(self, data: str) -> Any:
        """Decode TOML to a value."""
        ...

    def toml_encode(self, data: Any) -> str:
        """Encode a value to TOML."""
        ...

    def utf8_decode(self, data: bytes) -> str:
        """Decode UTF-8 bytes to a string."""
        ...

    def utf8_encode(self, data: str) -> bytes:
        """Encode a string to UTF-8 bytes."""
        ...

    def yaml_decode(self, data: str) -> Any:
        """Decode YAML to a value."""
        ...

    def yaml_encode(self, data: Any) -> str:
        """Encode a value to YAML."""
        ...


# Global handle instance.
_handle: Optional[Handle] = None


def set_handle(new_handle: Handle) -> None:
    """Set the global handle instance."""
    global _handle
    _handle = new_handle


def get_handle() -> Handle:
    """Get the global handle instance."""
    if _handle is None:
        raise RuntimeError("handle not set")
    return _handle
