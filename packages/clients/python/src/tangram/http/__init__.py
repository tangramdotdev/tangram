"""HTTP types and transport framing."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Self

from . import flow as flow
from .body import Body as Body
from .body import json_bytes as json_bytes
from .headers import Headers as Headers
from .request import Request as Request
from .response import Response as Response
from .uri import Uri as Uri
from .uri import percent_encode as percent_encode
from .uri import query_string as query_string


class Stream[T]:
    """Close an opened response even if its iterator has not been started."""

    def __init__(
        self, output: AsyncIterator[T], close: Callable[[], Awaitable[None]]
    ) -> None:
        self.output = output
        self._close = close

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        try:
            return await anext(self.output)
        except BaseException:
            await self.aclose()
            raise

    async def aclose(self) -> None:
        try:
            close = getattr(self.output, "aclose", None)
            if close is not None:
                await close()
        finally:
            await self._close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_) -> None:
        await self.aclose()
