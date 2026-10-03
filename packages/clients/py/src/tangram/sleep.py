"""Sleep using the active host."""

from __future__ import annotations

from . import host


async def sleep(duration: float) -> None:
    """Sleep for the specified duration in seconds."""
    return await host.sleep(duration)
