"""Buffer an asynchronous input independently of its consumers."""

import asyncio
from collections import deque
from collections.abc import AsyncIterable, Awaitable
from typing import Literal, Self, TypedDict


class IteratorValue[T](TypedDict):
    done: Literal[False]
    value: T


class IteratorEnd(TypedDict):
    done: Literal[True]
    value: None


type IteratorResult[T] = IteratorValue[T] | IteratorEnd


class Queue[T]:
    def __init__(self, input: AsyncIterable[T]):
        self._closed = False
        self._error: BaseException | None = None
        self._failed = False
        self._values: deque[T] = deque()
        self._waiters: deque[asyncio.Future[IteratorResult[T]]] = deque()
        self._task = asyncio.create_task(self._pump(input))

    async def _pump(self, input: AsyncIterable[T]):
        try:
            async for value in input:
                self._push(value)
            self._close()
        except BaseException as error:
            self._fail(error)

    def next(
        self, stop: Awaitable[None] | None = None
    ) -> asyncio.Future[IteratorResult[T]]:
        result: asyncio.Future[IteratorResult[T]] = (
            asyncio.get_running_loop().create_future()
        )
        if self._values:
            result.set_result({"done": False, "value": self._values.popleft()})
            return result
        if self._failed:
            assert self._error is not None
            result.set_exception(self._error)
            return result
        if self._closed:
            result.set_result({"done": True, "value": None})
            return result
        self._waiters.append(result)

        def remove_waiter(_):
            if result in self._waiters:
                self._waiters.remove(result)

        result.add_done_callback(remove_waiter)
        if stop is not None:
            stop = asyncio.ensure_future(stop)

            def stopped(signal):
                if signal.cancelled() or signal.exception() is not None:
                    return
                if result in self._waiters:
                    self._waiters.remove(result)
                    if not result.done():
                        result.set_result({"done": True, "value": None})

            stop.add_done_callback(stopped)
            result.add_done_callback(lambda _: stop.remove_done_callback(stopped))
        return result

    def _close(self):
        self._closed = True
        self._wake()

    def _fail(self, error: BaseException):
        self._error = error
        self._failed = True
        self._closed = True
        self._wake()

    def _push(self, value: T):
        if self._closed:
            return
        while self._waiters:
            waiter = self._waiters.popleft()
            if not waiter.done():
                waiter.set_result({"done": False, "value": value})
                return
        self._values.append(value)

    def _wake(self):
        while self._waiters:
            waiter = self._waiters.popleft()
            if waiter.done():
                continue
            if self._failed:
                assert self._error is not None
                waiter.set_exception(self._error)
            else:
                waiter.set_result({"done": True, "value": None})

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T:
        result = await self.next()
        if result["done"]:
            raise StopAsyncIteration
        return result["value"]
