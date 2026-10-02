"""Placeholders for build output paths."""

from __future__ import annotations

from typing import ClassVar, Self, TypedDict, cast

from .assert_ import assert_


def placeholder(name: str) -> Placeholder:
    """Create a placeholder."""
    return Placeholder(name)


class Placeholder:
    Data: ClassVar[type[Data]]
    __tangram_atomic__ = None

    def __init__(self, name: str) -> None:
        self._name = name

    @classmethod
    def expect(cls, value: object) -> Self:
        assert_(isinstance(value, cls))
        return cast(Self, value)

    @classmethod
    def assert_(cls, value: object) -> None:
        assert_(isinstance(value, cls))

    def to_data(self) -> Data:
        return {"name": self.name}

    @classmethod
    def from_data(cls, data: Data) -> Self:
        return cls(data["name"])

    @property
    def name(self) -> str:
        return self._name


class Data(TypedDict):
    name: str


Placeholder.Data = Data

output = placeholder("output")
