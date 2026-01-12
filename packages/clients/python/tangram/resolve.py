"""Deep async resolution utilities for Tangram."""

from __future__ import annotations

import asyncio
from typing import Any, Coroutine, Dict, List, TypeVar, Union

T = TypeVar("T")

Unresolved = Union[T, Coroutine[Any, Any, T], "UnresolvedList", "UnresolvedDict"]
UnresolvedList = List[Unresolved[Any]]
UnresolvedDict = Dict[str, Unresolved[Any]]


async def resolve(value: Unresolved[T]) -> T:
    """Recursively resolve all awaitables in a value.

    This function deeply resolves nested structures containing awaitables,
    using asyncio.gather for concurrent resolution.

    Examples:
        # Resolve a single awaitable.
        result = await resolve(some_async_func())

        # Resolve multiple awaitables concurrently.
        a, b, c = await resolve([async_a(), async_b(), async_c()])

        # Resolve nested structures.
        result = await resolve({
            "users": [get_user(1), get_user(2)],
            "config": get_config(),
        })
    """
    # Check if it's a coroutine first.
    if asyncio.iscoroutine(value):
        value = await value
        # After awaiting, recursively resolve the result.
        return await resolve(value)

    # Handle lists with concurrent resolution.
    if isinstance(value, list):
        resolved = await asyncio.gather(*[resolve(v) for v in value])
        return list(resolved)

    # Handle dicts with concurrent resolution.
    if isinstance(value, dict):
        keys = list(value.keys())
        values = await asyncio.gather(*[resolve(v) for v in value.values()])
        return dict(zip(keys, values))

    # Handle tuples with concurrent resolution.
    if isinstance(value, tuple):
        resolved = await asyncio.gather(*[resolve(v) for v in value])
        return tuple(resolved)

    # Check for awaitable objects (like those from async generators).
    if hasattr(value, "__await__"):
        awaited = await value
        return await resolve(awaited)

    # Return non-awaitable values as-is.
    return value


async def gather(*awaitables: Unresolved[Any]) -> List[Any]:
    """Resolve multiple values concurrently.

    This is a convenience wrapper around asyncio.gather that also
    handles nested resolution.

    Example:
        a, b, c = await gather(
            fetch_user(1),
            fetch_posts(user_id=1),
            fetch_config(),
        )
    """
    return await resolve(list(awaitables))


async def store_and_serialize(value: Any) -> Any:
    """Resolve awaitables, store objects, and convert to serializable form.

    This function:
    1. Resolves all awaitables in the value.
    2. Stores all Object_ instances to the server.
    3. Converts the value to a serializable form (Object_ -> its id).
    """
    from tangram.object_ import Object_

    # First resolve all awaitables.
    value = await resolve(value)

    # Then store and serialize.
    return await _store_and_serialize_inner(value)


async def _store_and_serialize_inner(value: Any) -> Any:
    """Recursively store objects and convert to serializable form."""
    from tangram.mutation import Mutation
    from tangram.object_ import Object_
    from tangram.placeholder import Placeholder
    from tangram.template import Template

    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        import base64

        return {"kind": "bytes", "value": base64.b64encode(value).decode("ascii")}
    # Handle Object_ instances - store and return proper serialized form.
    if Object_.is_(value):
        await value.store()
        return {"kind": "object", "value": value.id}
    # Handle Template.
    if isinstance(value, Template):
        # Store any objects in the template first.
        for obj in value.objects():
            await obj.store()
        return {"kind": "template", "value": value.to_data()}
    # Handle Mutation.
    if isinstance(value, Mutation):
        return {"kind": "mutation", "value": value.to_data()}
    # Handle Placeholder.
    if isinstance(value, Placeholder):
        return {"kind": "placeholder", "value": value.to_data()}
    if isinstance(value, list):
        results = await asyncio.gather(*[_store_and_serialize_inner(v) for v in value])
        return list(results)
    if isinstance(value, dict):
        keys = list(value.keys())
        values = await asyncio.gather(
            *[_store_and_serialize_inner(v) for v in value.values()]
        )
        return dict(zip(keys, values))
    if isinstance(value, tuple):
        results = await asyncio.gather(*[_store_and_serialize_inner(v) for v in value])
        return tuple(results)
    # For other types, return as-is and hope it is serializable.
    return value
