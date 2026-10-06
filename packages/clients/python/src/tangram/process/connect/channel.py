"""A streaming channel with independently bounded ordinary and priority queues."""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Any

from ...mutation import UNSET


class Channel[T]:
    def __init__(self, capacity: int):
        self.capacity = capacity
        self._closed = False
        self._error: BaseException | None = None
        self._priority: deque[T] = deque()
        self._values: deque[T] = deque()
        self._waiters: deque[asyncio.Future[dict[str, Any]]] = deque()

    def close(self, error: BaseException | None = None) -> None:
        self._closed = True
        self._error = error
        while self._waiters:
            waiter = self._waiters.popleft()
            if error is not None:
                waiter.set_exception(error)
            else:
                waiter.set_result({"done": True, "value": None})

    def next(self) -> asyncio.Future[dict[str, Any]]:
        value = self._priority.popleft() if self._priority else UNSET
        if value is UNSET or value is None:
            value = self._values.popleft() if self._values else UNSET
        result = asyncio.get_running_loop().create_future()
        if value is not UNSET:
            result.set_result({"done": False, "value": value})
        elif self._error is not None:
            result.set_exception(self._error)
        elif self._closed:
            result.set_result({"done": True, "value": None})
        else:
            self._waiters.append(result)
        return asyncio.shield(result)

    def push(self, value: T, priority: bool = False) -> bool:
        if self._closed:
            return False
        if self._waiters:
            self._waiters.popleft().set_result({"done": False, "value": value})
            return True
        values = self._priority if priority else self._values
        if len(values) >= self.capacity:
            raise RuntimeError("the process message queue is full")
        values.append(value)
        return True

    def return_(self) -> asyncio.Future[dict[str, Any]]:
        self.close()
        result = asyncio.get_running_loop().create_future()
        result.set_result({"done": True, "value": None})
        return result

    def __aiter__(self):
        return self

    async def __anext__(self) -> T:
        result = await self.next()
        if result["done"]:
            raise StopAsyncIteration
        return result["value"]
