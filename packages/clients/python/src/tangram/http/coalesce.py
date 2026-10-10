"""Coalesce immediately available HTTP body chunks."""

import asyncio
from collections.abc import AsyncGenerator, AsyncIterable


async def coalesce(
    body: AsyncIterable[bytes], size: int
) -> AsyncGenerator[bytes, None]:
    """Combine ready chunks and flush when the producer becomes pending."""
    iterator = body.__aiter__()
    pending = None
    finished = False
    try:
        while not finished:
            if pending is None:
                pending = asyncio.ensure_future(anext(iterator))
            try:
                chunk = await pending
            except StopAsyncIteration:
                return
            pending = None
            buffer = bytearray(chunk)
            while len(buffer) < size:
                pending = asyncio.ensure_future(anext(iterator))
                # Poll the next chunk without waiting for future input.
                if not pending.done():
                    await asyncio.sleep(0)
                if not pending.done():
                    break
                try:
                    buffer.extend(pending.result())
                except StopAsyncIteration:
                    finished = True
                except Exception:
                    # Send buffered bytes before surfacing the producer error.
                    break
                pending = None
                if finished:
                    break
            if buffer:
                yield bytes(buffer)
    finally:
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        close = getattr(iterator, "aclose", None)
        if close is not None:
            await close()
