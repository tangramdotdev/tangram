"""Range type.

This module mirrors the JavaScript packages/clients/js/src/range.ts.
"""

from typing import TypedDict

from tangram.position import Position


class Range(TypedDict):
    """A range in a file with start and end positions."""

    start: Position
    end: Position
