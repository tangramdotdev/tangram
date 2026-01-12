"""Handle implementation using tangram_syscall."""

from dataclasses import asdict
from typing import Any, Callable, Dict, Optional, Union

import _tangram_syscall as tangram_syscall


class Handle:
    """Handle implementation that wraps tangram_syscall."""

    async def read(self, arg: Any) -> bytes:
        # Convert dataclass to dict if needed.
        if hasattr(arg, "__dataclass_fields__"):
            arg = asdict(arg)
        return await tangram_syscall.read(arg)

    async def write(self, data: Union[str, bytes]) -> str:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return await tangram_syscall.write(data)

    async def get_object(self, id: str) -> Dict[str, Any]:
        return await tangram_syscall.object_get(id)

    async def post_object_batch(self, arg: Any) -> None:
        # Convert dataclass to dict if needed.
        if hasattr(arg, "__dataclass_fields__"):
            arg = asdict(arg)

        # arg["objects"] is already a list of {"id": id, "data": data}.
        await tangram_syscall.object_batch(arg)

    async def get_process(
        self, id: str, remote: Optional[str] = None
    ) -> Dict[str, Any]:
        return await tangram_syscall.process_get(id)

    async def spawn_process(self, arg: Dict[str, Any]) -> Dict[str, Any]:
        return await tangram_syscall.process_spawn(arg)

    async def wait_process(
        self, id: str, remote: Optional[str] = None
    ) -> Dict[str, Any]:
        return await tangram_syscall.process_wait(id)

    def checksum(self, data: Union[str, bytes], algorithm: str) -> str:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return tangram_syscall.checksum(data, algorithm)

    def object_id(self, data: Dict[str, Any]) -> str:
        return tangram_syscall.object_id(data)

    def log(self, stream: str, string: str) -> None:
        tangram_syscall.log(stream, string)

    async def sleep(self, duration: float) -> None:
        await tangram_syscall.sleep(duration)

    def magic(self, value: Callable[..., Any]) -> Dict[str, Any]:
        return tangram_syscall.magic(value)

    # Encoding functions.
    def base64_decode(self, data: str) -> bytes:
        return tangram_syscall.base64_decode(data)

    def base64_encode(self, data: bytes) -> str:
        return tangram_syscall.base64_encode(data)

    def hex_decode(self, data: str) -> bytes:
        return tangram_syscall.hex_decode(data)

    def hex_encode(self, data: bytes) -> str:
        return tangram_syscall.hex_encode(data)

    def json_decode(self, data: str) -> Any:
        return tangram_syscall.json_decode(data)

    def json_encode(self, data: Any) -> str:
        return tangram_syscall.json_encode(data)

    def toml_decode(self, data: str) -> Any:
        return tangram_syscall.toml_decode(data)

    def toml_encode(self, data: Any) -> str:
        return tangram_syscall.toml_encode(data)

    def utf8_decode(self, data: bytes) -> str:
        return tangram_syscall.utf8_decode(data)

    def utf8_encode(self, data: str) -> bytes:
        return tangram_syscall.utf8_encode(data)

    def yaml_decode(self, data: str) -> Any:
        return tangram_syscall.yaml_decode(data)

    def yaml_encode(self, data: Any) -> str:
        return tangram_syscall.yaml_encode(data)
