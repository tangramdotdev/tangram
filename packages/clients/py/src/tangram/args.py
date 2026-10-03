"""Resolve, map, and reduce constructor arguments, matching the JS Args module."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import cast

from .mutation import UNSET, Mutation, MutationKind, Unset
from .resolve import Unresolved, resolve
from .value import Value, ValueInput, ValueType

type Mapper[T] = Callable[[T], Mapping[str, object] | Awaitable[Mapping[str, object]]]
type Reducer = MutationKind | Callable[[object, object], object | Awaitable[object]]


class Args:
    @staticmethod
    async def apply[T](
        args: Sequence[Unresolved[T]], *, map: Mapper[T], reduce: Mapping[str, Reducer]
    ) -> dict[str, object]:
        return await Args.apply_resolved(
            args=cast(Sequence[T], await resolve(args)), map=map, reduce=reduce
        )

    @staticmethod
    async def apply_resolved[T](
        args: Sequence[T], *, map: Mapper[T], reduce: Mapping[str, Reducer]
    ) -> dict[str, object]:
        output: dict[str, object] = {}
        for arg in args:
            object_ = map(arg)
            if inspect.isawaitable(object_):
                object_ = await object_
            for key, value in object_.items():
                if value is UNSET:
                    continue
                elif value is None:
                    # An explicit null overrides any accumulated value, clearing it.
                    output[key] = None
                elif isinstance(value, Mutation):
                    current = output.get(key, UNSET)
                    if current is not UNSET:
                        Value.expect(current)
                    next_ = await value.apply(cast(ValueType | Unset, current))
                    if next_ is UNSET:
                        output.pop(key, None)
                    else:
                        output[key] = next_
                else:
                    reducer = reduce.get(key)
                    if isinstance(reducer, str):
                        match reducer:
                            case "set":
                                mutation = await Mutation.set(cast(ValueInput, value))
                            case "unset":
                                mutation = Mutation.unset()
                            case "set_if_unset":
                                mutation = await Mutation.set_if_unset(
                                    cast(ValueInput, value)
                                )
                            case "prepend":
                                mutation = await Mutation.prepend(
                                    cast(list[Unresolved[ValueType]], value)
                                )
                            case "append":
                                mutation = await Mutation.append(
                                    cast(list[Unresolved[ValueType]], value)
                                )
                            case "prefix" | "suffix":
                                from .directory import Directory
                                from .file import File
                                from .symlink import Symlink
                                from .template import Template

                                if not isinstance(
                                    value, (str, Template, Directory, File, Symlink)
                                ):
                                    raise TypeError("expected a template argument")
                                mutation = await getattr(Mutation, reducer)(value)
                            case "merge":
                                mutation = await Mutation.merge(
                                    cast(dict[str, ValueInput], value)
                                )
                            case _:
                                raise ValueError(f'unknown mutation kind "{reducer}"')
                        current = output.get(key, UNSET)
                        if current is not UNSET:
                            Value.expect(current)
                        next_ = await cast(Mutation[ValueType], mutation).apply(
                            cast(ValueType | Unset, current)
                        )
                        if next_ is UNSET:
                            output.pop(key, None)
                        else:
                            output[key] = next_
                    elif reducer is not None:
                        next_value = reducer(output.get(key, UNSET), value)
                        if inspect.isawaitable(next_value):
                            next_value = await next_value
                        output[key] = next_value
                    else:
                        output[key] = value
        return output
