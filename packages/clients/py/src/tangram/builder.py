from __future__ import annotations

from collections.abc import Generator
from typing import TYPE_CHECKING, Any, Self, cast

from .resolve import capture

if TYPE_CHECKING:
    from .client import Client


class Builder[T]:
    # The subclass supplies its constructor; individual factories type their args.
    type: Any = None
    _mode: str | None = None
    _join: bool = False

    def __init__(
        self, *args: Any, client: Client | None = None, **options: Any
    ) -> None:
        self._memo: dict[int, tuple[Any, Any]] = {}
        self._args = list(capture(args, self._memo))
        if options:
            self._args.append(capture(options, self._memo))
        self._client = client

    def _push(self, arg: Any) -> Self:
        self._args.append(capture(arg, self._memo))
        return self

    def __await__(self) -> Generator[Any, None, T]:
        return self._create().__await__()

    async def _create(self) -> T:
        return cast(T, await self.type.new(*self._args, client=self._client))
