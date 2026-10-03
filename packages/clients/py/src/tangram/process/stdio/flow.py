"""Flow control for process stdio reads."""

from __future__ import annotations

import math

chunk_size = 32 * 1024
max_chunks = 64
capacity = max_chunks * 2 + 4
window = chunk_size * max_chunks


class Receiver:
    """Track consumed bytes within one read attempt, independently of its position."""

    def __init__(self):
        self._chunks = 0
        self._consumed = 0
        self._reported = 0

    def consume(self, length: int) -> dict[str, int] | None:
        if length == 0:
            return None
        self._consumed += length
        self._chunks += 1
        if (
            abs(self._consumed) > 2**53 - 1
            or not math.isfinite(self._consumed)
            or self._consumed != int(self._consumed)
        ):
            raise ValueError("the stdio byte count is too large")
        if (
            self._consumed - self._reported < window / 2
            and self._chunks < max_chunks / 2
        ):
            return None
        self._reported = self._consumed
        self._chunks = 0
        return {"consumed": self._consumed}
