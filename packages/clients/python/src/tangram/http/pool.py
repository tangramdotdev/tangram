"""HTTP connection leases, held until their response bodies are released."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..config import PoolOptions

if TYPE_CHECKING:
    from ..host import Http2Session


@dataclass
class Entry:
    discarded: bool
    expires_at: float
    shared: int
    value: Http2Session


class Lease:
    def __init__(self, pool: Pool, entry: Entry):
        self.pool = pool
        self.entry = entry
        self.value = entry.value
        self.released = False

    def discard(self) -> None:
        self.entry.discarded = True
        self.pool.entries = [
            entry for entry in self.pool.entries if entry is not self.entry
        ]
        self.pool.schedule_expiration()
        self.pool.changed.set()

    def release(self) -> None:
        if self.released:
            return
        self.released = True
        self.entry.shared -= 1
        self.entry.expires_at = self.pool.expiration()
        if self.entry.discarded and self.entry.shared == 0:
            self.entry.value.retire()
        self.pool.schedule_expiration()
        self.pool.changed.set()


class Pool:
    def __init__(
        self, options: PoolOptions, create: Callable[[], Awaitable[Http2Session]]
    ):
        self.options = options
        self.create = create
        self.entries: list[Entry] = []
        self.pending = 0
        self.epoch = 0
        self.timer: asyncio.TimerHandle | None = None
        self.changed = asyncio.Event()

    async def get(self) -> Lease:
        while True:
            self.expire()
            self.changed.clear()
            for entry in self.entries:
                if entry.shared < self.options["shared"]:
                    entry.shared += 1
                    return Lease(self, entry)
            if len(self.entries) + self.pending < self.options["max"]:
                epoch = self.epoch
                self.pending += 1
                try:
                    value = await self.create()
                    if epoch != self.epoch:
                        value.retire()
                        continue
                    entry = Entry(False, self.expiration(), 1, value)
                    self.entries.append(entry)
                    return Lease(self, entry)
                finally:
                    self.pending -= 1
                    self.changed.set()
            await self.changed.wait()

    async def clear(self) -> None:
        self.epoch += 1
        if self.timer is not None:
            self.timer.cancel()
            self.timer = None
        entries, self.entries = self.entries, []
        for entry in entries:
            entry.discarded = True
            if entry.shared == 0:
                await entry.value.close()
        self.changed.set()

    def expire(self) -> None:
        for entry in self.entries.copy():
            if not entry.value.closed and (
                entry.shared != 0
                or len(self.entries) <= self.options["min"]
                or time.monotonic() < entry.expires_at
            ):
                continue
            entry.discarded = True
            self.entries.remove(entry)
            if entry.shared == 0:
                entry.value.retire()

    def schedule_expiration(self) -> None:
        if self.timer is not None:
            self.timer.cancel()
            self.timer = None
        if len(self.entries) <= self.options["min"]:
            return
        expires_at = min(
            (entry.expires_at for entry in self.entries if entry.shared == 0),
            default=float("inf"),
        )
        if expires_at == float("inf"):
            return
        self.timer = asyncio.get_running_loop().call_later(
            max(0, expires_at - time.monotonic()), self.expire_idle
        )

    def expire_idle(self) -> None:
        self.timer = None
        self.expire()
        self.changed.set()
        self.schedule_expiration()

    def expiration(self) -> float:
        ttl = self.options["ttl"]
        return float("inf") if ttl is None else time.monotonic() + ttl
