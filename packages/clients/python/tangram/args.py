"""Args pattern for handling variadic arguments with mutations.

This module mirrors the JavaScript packages/clients/js/src/args.ts.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, List, Optional, TypeVar, Union

from tangram.mutation import Mutation
from tangram.resolve import resolve

T = TypeVar("T")
O = TypeVar("O")


# Type alias for unresolved args.
Args = List[Any]


async def apply(
    args: Args,
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

    Example:
        result = await apply(
            args=[{"executable": "sh"}, {"args": ["-c", "echo hello"]}],
            map_fn=lambda arg: arg if isinstance(arg, dict) else {},
            reduce_spec={
                "args": "append",
                "env": "merge",
            },
        )
    """
    # Resolve all arguments.
    resolved = await resolve(args)

    output: Dict[str, Any] = {}

    for arg in resolved:
        # Map the argument to an object.
        obj = await map_fn(arg)

        # Reduce each key-value pair into the output.
        for key, value in obj.items():
            if isinstance(value, Mutation):
                # Apply the mutation directly.
                output = await value.apply(output, key)
            elif key in reduce_spec:
                reducer = reduce_spec[key]
                if isinstance(reducer, str):
                    # Create and apply a mutation based on the kind.
                    mutation = await _create_mutation(reducer, value)
                    output = await mutation.apply(output, key)
                else:
                    # Custom reducer function.
                    existing = output.get(key)
                    output[key] = await reducer(existing, value)
            else:
                # Default: just set the value.
                output[key] = value

    return output


async def _create_mutation(kind: str, value: Any) -> Mutation:
    """Create a mutation from a kind string and value."""
    if kind == "set":
        return await Mutation.set(value)
    elif kind == "unset":
        return Mutation.unset()
    elif kind == "set_if_unset":
        return await Mutation.set_if_unset(value)
    elif kind == "prepend":
        return await Mutation.prepend(value)
    elif kind == "append":
        return await Mutation.append(value)
    elif kind == "prefix":
        return await Mutation.prefix(value)
    elif kind == "suffix":
        return await Mutation.suffix(value)
    elif kind == "merge":
        return await Mutation.merge(value)
    else:
        raise ValueError(f"unknown mutation kind: {kind}")
