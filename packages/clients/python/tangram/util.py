"""Utility types for the Tangram client."""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    TypeVar,
    Union,
)

if TYPE_CHECKING:
    from tangram.mutation import Mutation

T = TypeVar("T")
A = TypeVar("A")
R = TypeVar("R")

# MaybeAwaitable: A value that may be synchronous or asynchronous.
MaybeAwaitable = Union[T, Awaitable[T]]

# Unresolved: A value that may contain promises or functions that need resolution.
Unresolved = Union[
    T,
    Awaitable["Unresolved[T]"],
    Callable[[], "Unresolved[T]"],
]

# Resolved: A fully resolved value (no promises or functions).
Resolved = T

# MaybeMutation: A value or a mutation of that value.
MaybeMutation = Union[T, "Mutation[T]"]

# MutationMap: A dictionary of optional mutations.
MutationMap = Dict[str, Optional["Mutation[Any]"]]

# ValueOrMaybeMutationMap: Either a value or a mutation map.
ValueOrMaybeMutationMap = Union[T, MutationMap]


class Args(Generic[T], List["Unresolved[ValueOrMaybeMutationMap[T]]"]):
    """Variadic arguments type.

    Matches the JavaScript tg.Args type for handling variadic arguments
    that may contain mutations.
    """

    @staticmethod
    async def apply(
        args: List[Any],
        map_fn: Callable[[Any], Awaitable[Dict[str, Any]]],
        reduce_spec: Dict[str, Union[str, Callable[[Any, Any], Awaitable[Any]]]],
    ) -> Dict[str, Any]:
        """Apply the Args pattern: resolve, map, and reduce arguments.

        This mirrors the JavaScript tg.Args.apply function.

        Args:
            args: List of unresolved arguments.
            map_fn: Async function to map each resolved arg to a dict.
            reduce_spec: Dict mapping keys to either:
                - A mutation kind string ("set", "append", "prepend", "merge", etc.)
                - A custom reducer function (a, b) -> result

        Returns:
            The merged output dict.
        """
        from tangram.args import apply

        return await apply(args, map_fn, reduce_spec)
