"""Encoding utilities matching JS encoding.ts."""

from typing import Any

from tangram.handle import get_handle


class base64:
    """Base64 encoding utilities."""

    @staticmethod
    def decode(value: str) -> bytes:
        """Decode a base64 string to bytes."""
        return get_handle().base64_decode(value)

    @staticmethod
    def encode(value: bytes) -> str:
        """Encode bytes to a base64 string."""
        return get_handle().base64_encode(value)


class hex:
    """Hex encoding utilities."""

    @staticmethod
    def decode(value: str) -> bytes:
        """Decode a hex string to bytes."""
        return get_handle().hex_decode(value)

    @staticmethod
    def encode(value: bytes) -> str:
        """Encode bytes to a hex string."""
        return get_handle().hex_encode(value)


class json:
    """JSON encoding utilities."""

    @staticmethod
    def decode(value: str) -> Any:
        """Decode a JSON string."""
        return get_handle().json_decode(value)

    @staticmethod
    def encode(value: Any) -> str:
        """Encode a value to JSON string."""
        return get_handle().json_encode(value)


class toml:
    """TOML encoding utilities."""

    @staticmethod
    def decode(value: str) -> Any:
        """Decode a TOML string."""
        return get_handle().toml_decode(value)

    @staticmethod
    def encode(value: Any) -> str:
        """Encode a value to TOML string."""
        return get_handle().toml_encode(value)


class utf8:
    """UTF-8 encoding utilities."""

    @staticmethod
    def decode(value: bytes) -> str:
        """Decode UTF-8 bytes to string."""
        return get_handle().utf8_decode(value)

    @staticmethod
    def encode(value: str) -> bytes:
        """Encode string to UTF-8 bytes."""
        return get_handle().utf8_encode(value)


class yaml:
    """YAML encoding utilities."""

    @staticmethod
    def decode(value: str) -> Any:
        """Decode a YAML string."""
        return get_handle().yaml_decode(value)

    @staticmethod
    def encode(value: Any) -> str:
        """Encode a value to YAML string."""
        return get_handle().yaml_encode(value)
