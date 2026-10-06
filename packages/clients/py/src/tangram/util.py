"""Public utility types corresponding to the JavaScript client's ``util.ts``.

Python supports awaitable unions and generic callables, but cannot express the
TypeScript mapped and conditional types used to transform every field in a
record or every element in a heterogeneous argument tuple. Map aliases therefore
take a value type, and argument aliases take an element type. Resolved aliases
take the resulting type directly; recursive resolution is performed by
``tangram.resolve``, rather than inferred by these aliases.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from .mutation import Mutation
from .referent import Referent
from .resolve import Unresolved
from .value import ValueType

type MaybePromise[T] = T | Awaitable[T]

type MaybeMutation[T: ValueType] = T | Mutation[T]

# Mutation is a runtime class whose payload type depends on its kind.
type MutationMap[T: ValueType] = dict[str, Mutation[T]]

type MaybeMutationMap[T: ValueType] = dict[str, MaybeMutation[T]]

# Preserve arbitrary T; explicit domain records model its mutable fields.
type ValueOrMaybeMutationMap[T] = T | dict[str, ValueType]

type MaybeReferent[T] = T | Referent[T]

type UnresolvedArgs[T] = list[Unresolved[T]] | tuple[Unresolved[T], ...]

type ResolvedArgs[T] = list[T] | tuple[T, ...]

type Function[**A, O] = Callable[A, O]

# Python's implicit function return and Tangram null are both None.
type ReturnValue[T] = MaybePromise[T]

type ResolvedReturnValue[T] = T

__all__ = [
    "MaybePromise",
    "MaybeMutation",
    "MutationMap",
    "MaybeMutationMap",
    "ValueOrMaybeMutationMap",
    "MaybeReferent",
    "UnresolvedArgs",
    "ResolvedArgs",
    "Function",
    "ReturnValue",
    "ResolvedReturnValue",
]
