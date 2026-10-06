"""The encoding interface and default standalone implementations."""

import base64 as _base64
import json as _json
import sys
import tomllib
from collections.abc import Mapping
from typing import Any, Never, NotRequired, Protocol, TypedDict, cast


class BytesEncoding(Protocol):
    def decode(self, value: str) -> bytes: ...
    def encode(self, value: bytes | bytearray | memoryview) -> str: ...


class ValueEncoding(Protocol):
    def decode(self, value: str) -> object: ...
    def encode(self, value: object) -> str: ...


class Utf8Encoding(Protocol):
    def decode(self, value: bytes | bytearray | memoryview) -> str: ...
    def encode(self, value: str) -> bytes: ...


class Encoding(Protocol):
    base64: BytesEncoding
    hex: BytesEncoding
    json: ValueEncoding
    toml: ValueEncoding
    utf8: Utf8Encoding
    yaml: ValueEncoding


class EncodingOperations(TypedDict):
    base64: NotRequired[BytesEncoding]
    hex: NotRequired[BytesEncoding]
    json: NotRequired[ValueEncoding]
    toml: NotRequired[ValueEncoding]
    utf8: NotRequired[Utf8Encoding]
    yaml: NotRequired[ValueEncoding]


encoding = sys.modules[__name__]


def set_encoding(new_encoding: Encoding | EncodingOperations) -> None:
    """Copy the new operations while preserving the shared encoding object."""
    if isinstance(new_encoding, Mapping):
        items = new_encoding.items()
    else:
        items = (
            (name, getattr(new_encoding, name))
            for name in dir(new_encoding)
            if not name.startswith("_")
        )
    for name, value in items:
        setattr(encoding, name, value)


class base64:
    @staticmethod
    def decode(value: str) -> bytes:
        result = _base64.b64decode(value, validate=True)
        # Reject nonzero padding bits just as the runtime's BASE64 decoder does.
        if _base64.b64encode(result).decode("ascii") != value:
            raise ValueError("invalid base64 padding")
        return result

    @staticmethod
    def encode(value: bytes | bytearray | memoryview) -> str:
        return _base64.b64encode(value).decode("ascii")


class hex:
    @staticmethod
    def decode(value: str) -> bytes:
        if any(character not in "0123456789abcdef" for character in value):
            raise ValueError("invalid hex digit")
        return bytes.fromhex(value)

    @staticmethod
    def encode(value: bytes | bytearray | memoryview) -> str:
        return bytes(value).hex()


class json:
    @staticmethod
    def decode(value: str) -> object:
        def reject_constant(value: str) -> Never:
            raise ValueError(f"invalid json constant {value}")

        return _json.loads(value, parse_constant=reject_constant)

    @staticmethod
    def encode(value: object) -> str:
        return _json.dumps(
            value, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        )


class toml:
    @staticmethod
    def decode(value: str) -> object:
        return tomllib.loads(value)

    @staticmethod
    def encode(value: object) -> str:
        import tomli_w

        return tomli_w.dumps(cast(Mapping[str, Any], value))


class utf8:
    @staticmethod
    def decode(value: bytes | bytearray | memoryview) -> str:
        return bytes(value).decode("utf-8")

    @staticmethod
    def encode(value: str) -> bytes:
        return value.encode("utf-8")


class yaml:
    @staticmethod
    def decode(value: str) -> object:
        import yaml

        return yaml.safe_load(value)

    @staticmethod
    def encode(value: object) -> str:
        import yaml

        return yaml.safe_dump(value, sort_keys=False)
