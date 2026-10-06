"""A character position in a source file."""

from typing import TypedDict


class Position(TypedDict):
    line: int
    character: int
