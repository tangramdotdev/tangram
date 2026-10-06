"""Value mutations and their awaitable constructor builder."""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    NotRequired,
    Self,
    TypedDict,
    cast,
    overload,
)

from .builder import Builder
from .resolve import Unresolved, resolve
from .template import Template, TemplateDataType, TemplateInput

if TYPE_CHECKING:
    from .object import Object
    from .value import ValueData, ValueInput, ValueType


class Unset:
    __slots__ = ()


# Distinguish a missing map entry from the Tangram null value.
UNSET = Unset()

type MutationKind = Literal[
    "set", "unset", "set_if_unset", "prepend", "append", "prefix", "suffix", "merge"
]


class SetArg(TypedDict):
    kind: Literal["set", "set_if_unset"]
    value: ValueInput


class UnsetArg(TypedDict):
    kind: Literal["unset"]


class ArrayArg(TypedDict):
    kind: Literal["prepend", "append"]
    values: Unresolved[list[ValueInput]]


class TemplateArg(TypedDict):
    kind: Literal["prefix", "suffix"]
    template: TemplateInput
    separator: NotRequired[str | None]


class MergeArg(TypedDict):
    kind: Literal["merge"]
    value: Unresolved[dict[str, ValueInput]]


type ArgType = SetArg | UnsetArg | ArrayArg | TemplateArg | MergeArg


class SetData(TypedDict):
    kind: Literal["set", "set_if_unset"]
    value: ValueData


class ArrayData(TypedDict):
    kind: Literal["prepend", "append"]
    values: list[ValueData]


class TemplateData(TypedDict):
    kind: Literal["prefix", "suffix"]
    template: TemplateDataType
    separator: NotRequired[str | None]


class MergeData(TypedDict):
    kind: Literal["merge"]
    value: dict[str, ValueData]


type DataType = SetData | UnsetArg | ArrayData | TemplateData | MergeData


class SetInner[T: ValueType](TypedDict):
    kind: Literal["set", "set_if_unset"]
    value: T


class ArrayInner(TypedDict):
    kind: Literal["prepend", "append"]
    values: list[ValueType]


class TemplateInner(TypedDict):
    kind: Literal["prefix", "suffix"]
    template: Template
    separator: NotRequired[str | None]


class MergeInner(TypedDict):
    kind: Literal["merge"]
    value: dict[str, ValueType | Unset]


type InnerType[T: ValueType] = (
    SetInner[T] | UnsetArg | ArrayInner | TemplateInner | MergeInner
)


class Mutation[T: ValueType]:
    Builder: ClassVar[type[MutationBuilder]]
    type Arg = ArgType
    type Inner[U: ValueType] = InnerType[U]
    type Kind = MutationKind
    __tangram_atomic__ = True
    Data: ClassVar[type[MutationData]]

    def __init__(
        self,
        inner: InnerType[T] | MutationKind,
        *,
        value: ValueType | dict[str, ValueType | Unset] = None,
        values: list[ValueType] | None = None,
        template: Template | None = None,
        separator: str | None = None,
    ) -> None:
        if isinstance(inner, dict):
            self._inner: dict[str, Any] = cast(dict[str, Any], inner)
        else:
            self._inner = {"kind": inner}
            if inner in ("set", "set_if_unset", "merge"):
                self._inner["value"] = value
            elif inner in ("prepend", "append"):
                self._inner["values"] = [] if values is None else values
            elif inner in ("prefix", "suffix"):
                self._inner["template"] = template
                self._inner["separator"] = separator

    @property
    def inner(self) -> InnerType[T]:
        return cast(InnerType[T], self._inner)

    @property
    def kind(self) -> MutationKind:
        return self._inner["kind"]

    @property
    def value(self) -> ValueType:
        return self._inner.get("value")

    @property
    def values(self) -> list[ValueType]:
        return self._inner.get("values", [])

    @property
    def template(self) -> Template | None:
        return self._inner.get("template")

    @property
    def separator(self) -> str | None:
        return self._inner.get("separator")

    @classmethod
    async def new(cls, arg: Unresolved[ArgType]) -> Mutation[ValueType]:
        arg = await resolve(arg)
        kind = arg["kind"]
        if kind == "unset":
            return cls.unset()
        if kind in ("prefix", "suffix"):
            return await getattr(cls, kind)(arg["template"], arg.get("separator"))
        if kind in ("append", "prepend"):
            return await getattr(cls, kind)(arg["values"])
        if kind in ("set", "set_if_unset", "merge"):
            return await getattr(cls, kind)(arg["value"])
        raise ValueError("invalid mutation kind")

    @classmethod
    @overload
    async def set[U: ValueType](cls, value: Unresolved[U]) -> Mutation[U]: ...

    @classmethod
    @overload
    async def set(cls, value: ValueInput) -> Mutation[ValueType]: ...

    @classmethod
    async def set(cls, value: ValueInput) -> Mutation[ValueType]:
        return cast("Mutation[ValueType]", cls("set", value=await resolve(value)))

    @classmethod
    def unset(cls) -> Mutation[ValueType]:
        return cast("Mutation[ValueType]", cls("unset"))

    @classmethod
    @overload
    async def set_if_unset[U: ValueType](cls, value: Unresolved[U]) -> Mutation[U]: ...

    @classmethod
    @overload
    async def set_if_unset(cls, value: ValueInput) -> Mutation[ValueType]: ...

    @classmethod
    async def set_if_unset(cls, value: ValueInput) -> Mutation[ValueType]:
        return cast(
            "Mutation[ValueType]", cls("set_if_unset", value=await resolve(value))
        )

    @classmethod
    async def prepend[U: ValueType](
        cls, values: Unresolved[list[Unresolved[U]]]
    ) -> Mutation[list[U]]:
        return cast(Mutation[list[U]], cls("prepend", values=await resolve(values)))

    @classmethod
    async def append[U: ValueType](
        cls, values: Unresolved[list[Unresolved[U]]]
    ) -> Mutation[list[U]]:
        return cast(Mutation[list[U]], cls("append", values=await resolve(values)))

    @classmethod
    async def prefix(
        cls, template: TemplateInput, separator: str | None = None
    ) -> Mutation[Template]:
        return cast(
            Mutation[Template],
            cls("prefix", template=await Template.new(template), separator=separator),
        )

    @classmethod
    async def suffix(
        cls, template: TemplateInput, separator: str | None = None
    ) -> Mutation[Template]:
        return cast(
            Mutation[Template],
            cls("suffix", template=await Template.new(template), separator=separator),
        )

    @classmethod
    async def merge(
        cls, value: Unresolved[dict[str, ValueInput]]
    ) -> Mutation[dict[str, ValueType]]:
        return cast(
            "Mutation[dict[str, ValueType]]", cls("merge", value=await resolve(value))
        )

    async def apply_to(self, target: dict[str, ValueType], key: str) -> None:
        value = await self._apply(target.get(key, UNSET))
        if value is UNSET:
            target.pop(key, None)
        else:
            target[key] = cast("ValueType", value)

    async def apply(self, value: T | Unset = UNSET) -> T | Unset:
        return cast(T | Unset, await self._apply(value))

    async def _apply(self, value: ValueType | Unset = UNSET) -> ValueType | Unset:
        if self.kind == "unset":
            return UNSET
        if self.kind == "set":
            return self.value
        if self.kind == "set_if_unset":
            return self.value if value is UNSET else value
        if self.kind in ("prepend", "append"):
            value = [] if value is UNSET or value is None else value
            if not isinstance(value, list):
                raise TypeError("expected an array mutation target")
            return (
                self.values + value if self.kind == "prepend" else value + self.values
            )
        if self.kind in ("prefix", "suffix"):
            from .artifact import Artifact

            value = await Template.new() if value is UNSET else value
            if value is not None and not isinstance(value, (str, Template, Artifact)):
                raise TypeError("expected a template mutation target")
            args = (
                (self.template, value)
                if self.kind == "prefix"
                else (value, self.template)
            )
            return await Template.join(
                self.separator, *(cast(TemplateInput, arg) for arg in args)
            )
        if self.kind == "merge":
            value = {} if value is UNSET or value is None else value
            if not isinstance(value, dict):
                raise TypeError("expected a map mutation target")
            for key, child in cast("dict[str, ValueType]", self.value).items():
                if child is UNSET:
                    continue
                if isinstance(child, Mutation):
                    await child.apply_to(value, key)
                else:
                    value[key] = child
            return value
        raise ValueError("invalid mutation kind")

    @classmethod
    def expect(cls, value: object) -> Self:
        if not isinstance(value, cls):
            raise TypeError("expected a mutation")
        return value

    @classmethod
    def assert_(cls, value: object) -> None:
        cls.expect(value)

    def objects(self) -> list[Object]:
        from .value import Value

        if self.kind in ("set", "set_if_unset"):
            return Value.objects(self.value)
        if self.kind in ("prepend", "append"):
            return Value.objects(self.values)
        if self.kind in ("prefix", "suffix"):
            return cast(Template, self.template).objects()
        return []

    def to_data(self) -> DataType:
        from .value import Value

        data = {"kind": self.kind}
        if self.kind in ("set", "set_if_unset"):
            data["value"] = Value.to_data(self.value)
        elif self.kind == "merge":
            data["value"] = {
                key: Value.to_data(value)
                for key, value in cast("dict[str, ValueType]", self.value).items()
            }
        elif self.kind in ("append", "prepend"):
            data["values"] = [Value.to_data(value) for value in self.values]
        elif self.kind in ("prefix", "suffix"):
            if self.template is None:
                raise ValueError("missing mutation template")
            data["template"] = self.template.to_data()
            if self.separator is not None:
                data["separator"] = self.separator
        elif self.kind != "unset":
            raise ValueError("invalid mutation kind")
        return cast("DataType", data)

    @classmethod
    def from_data(cls, data: DataType) -> Mutation[ValueType]:
        from .value import Value

        constructor = cast("type[Mutation[ValueType]]", cls)

        if data["kind"] == "unset":
            return constructor({"kind": data["kind"]})
        if data["kind"] == "set" or data["kind"] == "set_if_unset":
            return constructor(
                {"kind": data["kind"], "value": Value.from_data(data["value"])}
            )
        if data["kind"] == "prepend" or data["kind"] == "append":
            return constructor(
                {
                    "kind": data["kind"],
                    "values": [Value.from_data(value) for value in data["values"]],
                }
            )
        if data["kind"] == "prefix" or data["kind"] == "suffix":
            return constructor(
                {
                    "kind": data["kind"],
                    "template": Template.from_data(data["template"]),
                    "separator": data.get("separator"),
                }
            )
        if data["kind"] == "merge":
            return constructor(
                {
                    "kind": data["kind"],
                    "value": {
                        key: Value.from_data(value)
                        for key, value in data["value"].items()
                    },
                }
            )
        raise ValueError("invalid mutation kind")


class MutationData:
    type Type = DataType

    @staticmethod
    def children(data: DataType) -> list[str]:
        from .value import Value

        if data["kind"] == "unset":
            return []
        if data["kind"] == "set" or data["kind"] == "set_if_unset":
            return Value.Data.children(data["value"])
        if data["kind"] == "prepend" or data["kind"] == "append":
            return [
                child
                for value in data["values"]
                for child in Value.Data.children(value)
            ]
        if data["kind"] == "prefix" or data["kind"] == "suffix":
            return Template.Data.children(data["template"])
        if data["kind"] == "merge":
            return [
                child
                for value in data["value"].values()
                for child in Value.Data.children(value)
            ]
        raise ValueError("invalid mutation kind")

    @staticmethod
    def without_location_and_tokens(data: DataType) -> DataType:
        from .value import Value

        if data["kind"] == "unset":
            return cast("DataType", dict(data))
        if data["kind"] == "set" or data["kind"] == "set_if_unset":
            return cast(
                "DataType",
                {
                    **data,
                    "value": Value.Data.without_location_and_tokens(data["value"]),
                },
            )
        if data["kind"] == "prepend" or data["kind"] == "append":
            return cast(
                "DataType",
                {
                    **data,
                    "values": [
                        Value.Data.without_location_and_tokens(value)
                        for value in data["values"]
                    ],
                },
            )
        if data["kind"] == "prefix" or data["kind"] == "suffix":
            return cast(
                "DataType",
                {
                    **data,
                    "template": Template.Data.without_location_and_tokens(
                        data["template"]
                    ),
                },
            )
        if data["kind"] == "merge":
            return cast(
                "DataType",
                {
                    **data,
                    "value": {
                        key: Value.Data.without_location_and_tokens(value)
                        for key, value in data["value"].items()
                    },
                },
            )
        raise ValueError("invalid mutation kind")


class MutationBuilder(Builder["Mutation[ValueType]"]):
    type = Mutation

    def __init__(self, arg: Unresolved[ArgType]) -> None:
        super().__init__(arg)

    async def _create(self) -> Mutation[ValueType]:
        return await Mutation.new(self._args[0])


mutation = MutationBuilder
Mutation.Builder = MutationBuilder

Mutation.Data = MutationData
