"""A range of source positions."""

from typing import TypedDict

from .position import Position


class Range(TypedDict):
    start: Position
    end: Position
