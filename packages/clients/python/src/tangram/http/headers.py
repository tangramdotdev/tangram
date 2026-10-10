from __future__ import annotations

from collections.abc import Iterator, Mapping, MutableMapping
from typing import Any, overload

type HeaderValue = str | int | float | list[str]


class Headers(MutableMapping[str, HeaderValue]):
    def __init__(self, headers: Headers | Mapping[str, HeaderValue] | None = None):
        values = headers.to_data() if isinstance(headers, Headers) else headers or {}
        self._headers: MutableMapping[str, HeaderValue] = {
            name.lower(): value for name, value in values.items()
        }

    @overload
    def get(self, name: object, default: None = None) -> str | None: ...

    @overload
    def get[T](self, name: object, default: T) -> str | T: ...

    def get(self, name: object, default: Any = None) -> Any:
        value = self._headers.get(
            name.lower() if isinstance(name, str) else name, default
        )
        if isinstance(value, list):
            return value[0] if value else default
        if isinstance(value, (int, float)):
            if isinstance(value, float) and value.is_integer():
                return str(int(value))
            return str(value)
        return value

    def to_data(self) -> dict[str, HeaderValue]:
        return dict(self._headers)

    def __getitem__(self, name: str) -> HeaderValue:
        return self._headers[name.lower()]

    def __setitem__(self, name: str, value: HeaderValue) -> None:
        self._headers[name.lower()] = value

    def __delitem__(self, name: str) -> None:
        del self._headers[name.lower()]

    def __iter__(self) -> Iterator[str]:
        return iter(self._headers)

    def __len__(self) -> int:
        return len(self._headers)
