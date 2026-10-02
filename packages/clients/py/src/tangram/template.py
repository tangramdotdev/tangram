"""Artifact-aware templates and their awaitable constructor builder."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Literal, Self, TypedDict, cast

from .assert_ import assert_
from .builder import Builder
from .placeholder import Placeholder
from .referent import Referent
from .resolve import Unresolved, resolve

if TYPE_CHECKING:
    from .directory import Directory
    from .file import File
    from .object import Object
    from .placeholder import Data as PlaceholderData
    from .symlink import Symlink

type TemplateComponent = str | Directory | File | Symlink | Placeholder
type TemplateArg = None | TemplateComponent | Template
type TemplateInput = Unresolved[TemplateArg]


class StringData(TypedDict):
    kind: Literal["string"]
    value: str


class ArtifactData(TypedDict):
    kind: Literal["artifact"]
    value: str


class PlaceholderComponentData(TypedDict):
    kind: Literal["placeholder"]
    value: PlaceholderData


type ComponentData = StringData | ArtifactData | PlaceholderComponentData


class TemplateDataType(TypedDict):
    components: list[ComponentData]


class Template:
    Builder: ClassVar[type[TemplateBuilder]]
    Data: ClassVar[type[TemplateData]]
    type Component = TemplateComponent
    type Arg = TemplateArg
    __tangram_atomic__ = True

    def __init__(self, components: list[TemplateComponent]) -> None:
        self._components = components

    @classmethod
    async def new(cls, *args: TemplateInput) -> Self:
        return cls._new_resolved(*(await resolve(args)))

    @classmethod
    def expect(cls, value: object) -> Self:
        assert_(isinstance(value, cls))
        return cast(Self, value)

    @classmethod
    def assert_(cls, value: object) -> None:
        cls.expect(value)

    def to_data(self) -> TemplateDataType:
        from .object import Object

        components: list[ComponentData] = []
        for component in self.components:
            if isinstance(component, str):
                components.append({"kind": "string", "value": component})
            elif isinstance(component, Placeholder):
                components.append({"kind": "placeholder", "value": component.to_data()})
            elif isinstance(component, Object) and component.kind in (
                "directory",
                "file",
                "symlink",
            ):
                components.append(
                    {
                        "kind": "artifact",
                        "value": component.to_referent().to_data_string(),
                    }
                )
            else:
                raise TypeError("invalid template component")
        return {"components": components}

    @classmethod
    def from_data(cls, data: TemplateDataType) -> Self:
        from .object import Object

        components: list[TemplateComponent] = []
        for component in data["components"]:
            if component["kind"] == "string":
                components.append(component["value"])
            elif component["kind"] == "placeholder":
                components.append(Placeholder(component["value"]["name"]))
            elif component["kind"] == "artifact":
                artifact = Object.with_referent(
                    Referent.from_data_string(component["value"])
                )
                components.append(cast(TemplateComponent, artifact))
            else:
                raise ValueError("invalid template component kind")
        return cls(components)

    def objects(self) -> list[Object]:
        return [
            component
            for component in self._components
            if not isinstance(component, (str, Placeholder))
        ]

    @classmethod
    def join(cls, separator: TemplateInput, *args: TemplateInput) -> TemplateBuilder:
        return TemplateBuilder.join(separator, *args)

    @property
    def components(self) -> list[TemplateComponent]:
        return list(self._components)

    @staticmethod
    def raw(strings: list[str], *placeholders: TemplateInput) -> TemplateBuilder:
        return TemplateBuilder(True, strings, *placeholders)

    @classmethod
    def _new_resolved(cls, *args: TemplateArg) -> Self:
        from .directory import Directory
        from .file import File
        from .mutation import UNSET
        from .symlink import Symlink

        components: list[TemplateComponent] = []
        for arg in args:
            if arg is None or arg is UNSET:
                continue
            components_arg = arg.components if isinstance(arg, cls) else [arg]
            for component in components_arg:
                if not isinstance(
                    component, (str, Directory, File, Symlink, Placeholder)
                ):
                    raise TypeError("invalid template component")
                if isinstance(component, str) and not component:
                    continue
                if (
                    components
                    and isinstance(components[-1], str)
                    and isinstance(component, str)
                ):
                    components[-1] += component
                else:
                    components.append(component)
        return cls(components)

    @classmethod
    def _join_resolved(cls, separator: TemplateArg, *args: TemplateArg) -> Self:
        templates = [cls._new_resolved(arg) for arg in args]
        templates = [arg for arg in templates if arg.components]
        components: list[TemplateArg] = []
        for arg in templates:
            if components:
                components.append(separator)
            components.append(arg)
        return cls._new_resolved(*components)


class TemplateBuilder(Builder[Template]):
    type = Template

    def __init__(self, *args: TemplateInput | list[str] | bool) -> None:
        arguments = list(args)
        raw = False
        if arguments and isinstance(arguments[0], bool):
            raw = arguments.pop(0)
        if (
            arguments
            and isinstance(arguments[0], list)
            and hasattr(arguments[0], "raw")
        ):
            strings = cast(list[str], arguments[0])
            placeholders = cast(list[TemplateInput], arguments[1:])
            strings = list(strings) if raw else unindent(list(strings))
            components: list[TemplateInput] = []
            for index, string in enumerate(strings[:-1]):
                components.extend([string, placeholders[index]])
            components.append(strings[-1])
            super().__init__(*components)
        else:
            super().__init__(*arguments)

    @classmethod
    def join(cls, separator: TemplateInput, *args: TemplateInput) -> TemplateBuilder:
        builder = cls(separator, *args)
        builder._join = True
        return builder

    async def _create(self) -> Template:
        args = await resolve(self._args)
        if getattr(self, "_join", False):
            separator, *args = args
            return Template._join_resolved(separator, *args)
        return Template._new_resolved(*args)


class TemplateData:
    type Type = TemplateDataType
    type Component = ComponentData

    @staticmethod
    def children(data: TemplateDataType) -> list[str]:
        return [
            Referent.from_data_string(component["value"]).node
            for component in data["components"]
            if component["kind"] == "artifact"
        ]

    @staticmethod
    def without_location_and_tokens(data: TemplateDataType) -> TemplateDataType:
        components: list[ComponentData] = []
        for component in data["components"]:
            if component["kind"] == "artifact":
                referent = Referent.from_data_string(component["value"])
                component = ArtifactData(
                    kind="artifact",
                    value=referent.without_location_and_tokens().to_data_string(),
                )
            components.append(component)
        return {**data, "components": components}


def raw(strings: list[str], *placeholders: TemplateInput) -> TemplateBuilder:
    return TemplateBuilder(True, strings, *placeholders)


def unindent(strings: list[str]) -> list[str]:
    # Concatenate the strings and collect the placeholder indices.
    placeholder_indices = []
    string = strings[0]
    for component in strings[1:]:
        placeholder_indices.append(len(string))
        string += component

    # Split the string into lines.
    lines = string.split("\n")

    # Compute the indentation.
    position = 0
    counts = []
    for index, line in enumerate(lines):
        if index:
            first_non_whitespace = next(
                (index for index, char in enumerate(line) if char not in " \t"),
                float("inf"),
            )
            first_placeholder = next(
                (
                    index - position
                    for index in placeholder_indices
                    if position <= index < position + len(line) + 1
                ),
                float("inf"),
            )
            count = min(first_non_whitespace, first_placeholder)
            if count != float("inf"):
                counts.append(count)
        position += len(line) + 1
    indentation = min(counts, default=float("inf"))

    # Remove an empty first line and update the placeholder indices.
    if string.startswith("\n") and (
        not placeholder_indices or placeholder_indices[0] != 0
    ):
        string = string[1:]
        lines = lines[1:]
        placeholder_indices = [index - 1 for index in placeholder_indices]

    # Unindent each line and update the placeholder indices.
    position = 0
    for index, line in enumerate(lines):
        first_non_whitespace = next(
            (index for index, char in enumerate(line) if char not in " \t"),
            float("inf"),
        )
        first_placeholder = next(
            (
                index - position
                for index in placeholder_indices
                if position <= index < position + len(line)
            ),
            float("inf"),
        )
        remove = int(
            min(indentation, len(line), first_non_whitespace, first_placeholder)
        )
        lines[index] = line[remove:]
        placeholder_indices = [
            index - remove if index >= position else index
            for index in placeholder_indices
        ]
        position += len(lines[index]) + 1

    # Join the lines and split the string at the placeholder indices.
    string = "\n".join(lines)
    output = []
    index = 0
    for next_index in placeholder_indices:
        output.append(string[index:next_index])
        index = next_index
    output.append(string[index:])
    return output


Template.Data = TemplateData
Template.Builder = TemplateBuilder
template = TemplateBuilder
