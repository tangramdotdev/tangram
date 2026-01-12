"""Mutation class for immutable data transformations."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Optional,
    TypeVar,
    Union,
)

T = TypeVar("T")


class MutationKind(Enum):
    """Kinds of mutations."""

    SET = "set"
    UNSET = "unset"
    SET_IF_UNSET = "set_if_unset"
    PREPEND = "prepend"
    APPEND = "append"
    PREFIX = "prefix"
    SUFFIX = "suffix"
    MERGE = "merge"


@dataclass
class MutationInner:
    """Internal mutation data."""

    kind: MutationKind
    value: Optional[Any] = None
    values: Optional[List[Any]] = None
    template: Optional[Any] = None
    separator: Optional[str] = None


class Mutation(Generic[T]):
    """Immutable data transformation."""

    _inner: MutationInner

    def __init__(self, inner: MutationInner) -> None:
        """Initialize a mutation."""
        self._inner = inner

    @property
    def kind(self) -> MutationKind:
        """Get the mutation kind."""
        return self._inner.kind

    @staticmethod
    async def set(value: T) -> Mutation[T]:
        """Create a set mutation."""
        from tangram.resolve import resolve

        value = await resolve(value)
        return Mutation(MutationInner(kind=MutationKind.SET, value=value))

    @staticmethod
    def unset() -> Mutation[T]:
        """Create an unset mutation."""
        return Mutation(MutationInner(kind=MutationKind.UNSET))

    @staticmethod
    async def set_if_unset(value: T) -> Mutation[T]:
        """Create a set-if-unset mutation."""
        from tangram.resolve import resolve

        value = await resolve(value)
        return Mutation(MutationInner(kind=MutationKind.SET_IF_UNSET, value=value))

    @staticmethod
    async def prepend(value: Any) -> Mutation[List[Any]]:
        """Create a prepend mutation."""
        from tangram.resolve import resolve

        value = await resolve(value)
        values = value if isinstance(value, list) else [value]
        return Mutation(MutationInner(kind=MutationKind.PREPEND, values=values))

    @staticmethod
    async def append(value: Any) -> Mutation[List[Any]]:
        """Create an append mutation."""
        from tangram.resolve import resolve

        value = await resolve(value)
        values = value if isinstance(value, list) else [value]
        return Mutation(MutationInner(kind=MutationKind.APPEND, values=values))

    @staticmethod
    async def prefix(template: Any, separator: Optional[str] = None) -> Mutation[str]:
        """Create a prefix mutation."""
        from tangram.resolve import resolve

        template = await resolve(template)
        return Mutation(
            MutationInner(
                kind=MutationKind.PREFIX, template=template, separator=separator
            )
        )

    @staticmethod
    async def suffix(template: Any, separator: Optional[str] = None) -> Mutation[str]:
        """Create a suffix mutation."""
        from tangram.resolve import resolve

        template = await resolve(template)
        return Mutation(
            MutationInner(
                kind=MutationKind.SUFFIX, template=template, separator=separator
            )
        )

    @staticmethod
    async def merge(value: Dict[str, Any]) -> Mutation[Dict[str, Any]]:
        """Create a merge mutation."""
        from tangram.resolve import resolve

        value = await resolve(value)
        return Mutation(MutationInner(kind=MutationKind.MERGE, value=value))

    async def apply(self, obj: Dict[str, Any], key: str) -> Dict[str, Any]:
        """Apply this mutation to an object at the given key.

        Returns the modified object (may be a new object).
        """
        result = dict(obj)  # Shallow copy.
        kind = self._inner.kind

        if kind == MutationKind.SET:
            result[key] = self._inner.value
        elif kind == MutationKind.UNSET:
            result.pop(key, None)
        elif kind == MutationKind.SET_IF_UNSET:
            if key not in result:
                result[key] = self._inner.value
        elif kind == MutationKind.PREPEND:
            existing = result.get(key, [])
            if not isinstance(existing, list):
                existing = [existing] if existing is not None else []
            result[key] = (self._inner.values or []) + existing
        elif kind == MutationKind.APPEND:
            existing = result.get(key, [])
            if not isinstance(existing, list):
                existing = [existing] if existing is not None else []
            result[key] = existing + (self._inner.values or [])
        elif kind == MutationKind.PREFIX:
            from tangram.template import Template, template as make_template

            existing = result.get(key)
            if existing is None:
                result[key] = self._inner.template
            else:
                sep = self._inner.separator or ""
                result[key] = make_template(self._inner.template, sep, existing)
        elif kind == MutationKind.SUFFIX:
            from tangram.template import Template, template as make_template

            existing = result.get(key)
            if existing is None:
                result[key] = self._inner.template
            else:
                sep = self._inner.separator or ""
                result[key] = make_template(existing, sep, self._inner.template)
        elif kind == MutationKind.MERGE:
            existing = result.get(key, {})
            if not isinstance(existing, dict):
                existing = {}
            merged = dict(existing)
            merged.update(self._inner.value or {})
            result[key] = merged

        return result

    def to_data(self) -> Dict[str, Any]:
        """Convert to serializable data."""
        from tangram.value import to_data

        data: Dict[str, Any] = {"kind": self._inner.kind.value}
        if self._inner.value is not None:
            data["value"] = to_data(self._inner.value)
        if self._inner.values is not None:
            data["values"] = [to_data(v) for v in self._inner.values]
        if self._inner.template is not None:
            data["template"] = to_data(self._inner.template)
        if self._inner.separator is not None:
            data["separator"] = self._inner.separator
        return data

    @staticmethod
    def from_data(data: Dict[str, Any]) -> Mutation[Any]:
        """Create a mutation from serialized data."""
        from tangram.value import from_data

        kind = MutationKind(data["kind"])
        value = from_data(data["value"]) if "value" in data else None
        values = [from_data(v) for v in data["values"]] if "values" in data else None
        template = from_data(data["template"]) if "template" in data else None
        separator = data.get("separator")
        return Mutation(
            MutationInner(
                kind=kind,
                value=value,
                values=values,
                template=template,
                separator=separator,
            )
        )
