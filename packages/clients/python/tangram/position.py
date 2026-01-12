"""Position type.

This module mirrors the JavaScript packages/clients/js/src/position.ts.
"""

from typing import TypedDict


class Position(TypedDict):
    """A position in a file with line and character."""

    line: int
    character: int
