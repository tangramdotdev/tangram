"""Render values using the JavaScript client's Printer structure."""

from __future__ import annotations

import base64
import json
import math
from collections.abc import Callable, Mapping, Sequence
from decimal import Decimal
from typing import TYPE_CHECKING, Any, TypedDict, cast

if TYPE_CHECKING:
    from ..blob import Blob, BlobChild, BlobLeaf, BlobValue
    from ..command import Command, CommandObjectValue, CommandValue, ExecutableObject
    from ..diagnostic import DiagnosticObject
    from ..directory import (
        Directory,
        DirectoryBranch,
        DirectoryChild,
        DirectoryLeaf,
        DirectoryValue,
    )
    from ..error import Error, ErrorFileObject, ErrorLocationObject, ErrorObjectValue
    from ..file import File, FileValue
    from ..graph import (
        DirectoryNode,
        FileNode,
        Graph,
        GraphNode,
        GraphValue,
        SymlinkNode,
    )
    from ..module import ModuleLocationObject
    from ..mutation import Unset
    from ..object import ObjectState
    from ..position import Position
    from ..range import Range
    from ..referent import Options as ReferentOptions
    from ..symlink import Symlink, SymlinkValue
    from . import ValueType

from ..graph import Pointer
from ..module import Module
from ..mutation import UNSET, Mutation
from ..object import Object
from ..placeholder import Placeholder
from ..referent import Referent
from ..template import Template

# Internal mutation records can include the missing-value sentinel.
type Printable = ValueType | Unset | Sequence[Printable] | Mapping[str, Printable]


class Options(TypedDict, total=False):
    color: bool | None
    indent: int | None
    indentation: str | None


class Printer:
    def __init__(self, options: Options | None = None) -> None:
        options = options or {}
        self.color = options.get("color") or False
        self.indent_ = options.get("indent") or 0
        self.indentation = options.get("indentation")

    def print(self, value: ValueType) -> str:
        return self.value(value)

    def value(self, value: Printable) -> str:
        if isinstance(value, str):
            return self.string(value)
        if isinstance(value, bool):
            return self.style("true" if value else "false", colors["yellow"])
        if isinstance(value, (int, float)):
            return self.style(number_string(value), colors["yellow"])
        if value is UNSET:
            return self.style("undefined", colors["gray"])
        if value is None:
            return self.null()
        if isinstance(value, Sequence) and not isinstance(
            value, (str, bytes, bytearray, memoryview)
        ):
            return self.array(
                [lambda value=value: self.value(value) for value in value]
            )
        if isinstance(value, Object):
            return self.object_handle(value)
        if isinstance(value, (bytes, bytearray, memoryview)):
            return self.call("bytes", self.string(base64.b64encode(value).decode()))
        if isinstance(value, Mutation):
            return self.mutation(value)
        if isinstance(value, Module):
            return self.call("module", self.module(value))
        if isinstance(value, Template):
            return self.template(value)
        if isinstance(value, Placeholder):
            return self.placeholder(value)
        if isinstance(value, Mapping):
            return self.map(
                {
                    key: lambda child=child: self.value(child)
                    for key, child in value.items()
                }
            )
        raise TypeError("invalid value")

    def null(self) -> str:
        return self.style("null", colors["gray"])

    def blob(self, value: Blob) -> str:
        state = value.state
        object_ = state.object
        if object_ is None:
            return self.object_id(state)
        assert object_["kind"] == "blob", "expected a blob object"
        return self.blob_object(object_["value"])

    def directory(self, value: Directory) -> str:
        state = value.state
        object_ = state.object
        if object_ is None:
            return self.object_id(state)
        assert object_["kind"] == "directory", "expected a directory object"
        return self.directory_object(object_["value"])

    def file(self, value: File) -> str:
        state = value.state
        object_ = state.object
        if object_ is None:
            return self.object_id(state)
        assert object_["kind"] == "file", "expected a file object"
        return self.file_object(object_["value"])

    def symlink(self, value: Symlink) -> str:
        state = value.state
        object_ = state.object
        if object_ is None:
            return self.object_id(state)
        assert object_["kind"] == "symlink", "expected a symlink object"
        return self.symlink_object(object_["value"])

    def graph(self, value: Graph) -> str:
        state = value.state
        object_ = state.object
        if object_ is None:
            return self.object_id(state)
        assert object_["kind"] == "graph", "expected a graph object"
        return self.graph_object(object_["value"])

    def command(self, value: Command) -> str:
        state = value.state
        object_ = state.object
        if object_ is None:
            return self.object_id(state)
        assert object_["kind"] == "command", "expected a command object"
        return self.command_object(object_["value"])

    def error(self, value: Error) -> str:
        state = value.state
        object_ = state.object
        if object_ is None:
            return self.object_id(state)
        assert object_["kind"] == "error", "expected an error object"
        return self.error_object(object_["value"])

    def object_handle(self, value: Object) -> str:
        from ..blob import Blob
        from ..command import (
            Command,
        )
        from ..directory import Directory
        from ..file import File
        from ..graph import (
            Graph,
        )
        from ..symlink import Symlink

        if isinstance(value, Blob):
            return self.blob(value)
        if isinstance(value, Directory):
            return self.directory(value)
        if isinstance(value, File):
            return self.file(value)
        if isinstance(value, Symlink):
            return self.symlink(value)
        if isinstance(value, Graph):
            return self.graph(value)
        if isinstance(value, Command):
            return self.command(value)
        return self.error(cast("Error", value))

    def artifact(self, value: Directory | File | Symlink) -> str:
        from ..directory import Directory
        from ..file import File

        if isinstance(value, Directory):
            return self.directory(value)
        if isinstance(value, File):
            return self.file(value)
        return self.symlink(value)

    def blob_object(self, object_: BlobValue) -> str:
        if "bytes" in object_:
            try:
                string = bytes(cast("BlobLeaf", object_)["bytes"]).decode(
                    "utf-8", errors="replace"
                )
                if self.bytes_equal(
                    string.encode("utf-8"), cast("BlobLeaf", object_)["bytes"]
                ):
                    return self.call("blob", self.value(string))
            except UnicodeError:
                return self.call("blob")
            return self.call("blob")
        children = object_["children"]
        if len(children) == 0:
            return self.call("blob", self.map({}))
        if len(children) == 1:
            return self.call("blob", self.blob_child(children[0]))
        return self.call(
            "blob",
            self.map(
                {
                    "children": lambda: self.array(
                        [
                            lambda child=child: self.blob_child(child)
                            for child in children
                        ]
                    )
                }
            ),
        )

    def blob_child(self, child: BlobChild) -> str:
        return self.map(
            {
                "length": lambda: self.value(field(child, "length")),
                "blob": lambda: self.blob(field(child, "blob")),
            }
        )

    def directory_object(self, object_: DirectoryValue) -> str:
        if Pointer.is_(object_):
            return self.call("directory", self.graph_pointer(object_))
        return self.call(
            "directory",
            self.directory_node_entries(
                cast("DirectoryLeaf | DirectoryBranch", object_)
            ),
        )

    def directory_node_entries(
        self, directory: DirectoryLeaf | DirectoryBranch | DirectoryNode
    ) -> str | None:
        if "entries" in directory:
            entries = {
                name: lambda edge=edge: self.graph_edge_artifact(edge)
                for name, edge in cast("DirectoryLeaf", directory)["entries"].items()
            }
            return self.map(entries) if entries else None
        return self.map(
            {
                "children": lambda: self.array(
                    [
                        lambda child=child: self.graph_directory_child(child)
                        for child in cast("DirectoryBranch", directory)["children"]
                    ]
                )
            }
        )

    def graph_directory(self, directory: DirectoryNode, tag: bool) -> str:
        entries = {}
        if tag:
            entries["kind"] = lambda: self.value("directory")
        if "entries" in directory:
            directory_entries = {
                name: lambda edge=edge: self.graph_edge_artifact(edge)
                for name, edge in cast("DirectoryLeaf", directory)["entries"].items()
            }
            if directory_entries:
                entries["entries"] = lambda: self.map(directory_entries)
        else:
            entries["children"] = lambda: self.array(
                [
                    lambda child=child: self.graph_directory_child(child)
                    for child in cast("DirectoryBranch", directory)["children"]
                ]
            )
        return self.map(entries)

    def graph_directory_child(self, child: DirectoryChild) -> str:
        return self.map(
            {
                "directory": lambda: self.graph_edge_directory(
                    field(child, "directory")
                ),
                "count": lambda: self.value(field(child, "count")),
                "last": lambda: self.value(field(child, "last")),
            }
        )

    def file_object(self, object_: FileValue | Pointer) -> str:
        if Pointer.is_(object_):
            return self.call("file", self.graph_pointer(object_))
        return self.call("file", self.graph_file(cast("FileNode", object_), False))

    def graph_file(self, file: FileNode, tag: bool) -> str:
        entries = {}
        if tag:
            entries["kind"] = lambda: self.value("file")
        entries["contents"] = lambda: self.blob(file["contents"])
        dependencies = file.get("dependencies") or {}
        if dependencies:
            entries["dependencies"] = lambda: self.map(
                {
                    reference: lambda dependency=dependency: self.graph_dependency(
                        dependency
                    )
                    for reference, dependency in dependencies.items()
                }
            )
        if file.get("executable"):
            entries["executable"] = lambda: self.value(file["executable"])
        if file.get("module") is not None:
            entries["module"] = lambda: self.value(file["module"])
        return self.map(entries)

    def graph_dependency(
        self, dependency: Referent[Object | Pointer | int] | None
    ) -> str:
        if dependency is None:
            return self.null()
        entries = {}
        node = field(dependency, "node")
        if node is not None:
            entries["node"] = lambda: self.graph_edge_object(node)
        options = field(dependency, "options")
        if self.has_referent_options(options):
            entries["options"] = lambda: self.referent_options(options)
        return self.map(entries)

    def symlink_object(self, object_: SymlinkValue | Pointer) -> str:
        if Pointer.is_(object_):
            return self.call("symlink", self.graph_pointer(object_))
        return self.call(
            "symlink", self.graph_symlink(cast("SymlinkNode", object_), False)
        )

    def graph_symlink(self, symlink: SymlinkNode, tag: bool) -> str:
        entries = {}
        if tag:
            entries["kind"] = lambda: self.value("symlink")
        if symlink.get("artifact") is not None:
            entries["artifact"] = lambda: self.graph_edge_artifact(
                cast("Directory | File | Symlink | Pointer | int", symlink["artifact"])
            )
        if symlink.get("path") is not None:
            entries["path"] = lambda: self.value(symlink["path"])
        return self.map(entries)

    def graph_object(self, object_: GraphValue) -> str:
        entries = {}
        nodes = object_["nodes"]
        if nodes:
            entries["nodes"] = lambda: self.array(
                [lambda node=node: self.graph_node(node) for node in nodes]
            )
        return self.call("graph", self.map(entries))

    def graph_node(self, node: GraphNode) -> str:
        if node["kind"] == "directory":
            return self.graph_directory(node, True)
        if node["kind"] == "file":
            return self.graph_file(node, True)
        if node["kind"] == "symlink":
            return self.graph_symlink(node, True)
        raise ValueError("invalid graph node kind")

    def graph_edge_object(self, edge: Object | Pointer | int) -> str:
        if isinstance(edge, (int, float)):
            return self.value(edge)
        if Pointer.is_(edge):
            return self.graph_pointer(edge)
        return self.object_handle(cast(Object, edge))

    def graph_edge_artifact(
        self, edge: Directory | File | Symlink | Pointer | int
    ) -> str:
        if isinstance(edge, (int, float)):
            return self.value(edge)
        if Pointer.is_(edge):
            return self.graph_pointer(edge)
        return self.artifact(cast("Directory | File | Symlink", edge))

    def graph_edge_directory(self, edge: Directory | Pointer | int) -> str:
        if isinstance(edge, (int, float)):
            return self.value(edge)
        if Pointer.is_(edge):
            return self.graph_pointer(edge)
        return self.directory(cast("Directory", edge))

    def graph_pointer(self, pointer: Pointer) -> str:
        return self.map(
            {
                "graph": lambda: self.graph(field(pointer, "graph")),
                "index": lambda: self.value(field(pointer, "index")),
                "kind": lambda: self.value(field(pointer, "kind")),
            }
        )

    def command_object(self, object_: CommandObjectValue) -> str:
        entries = {}
        if object_.get("args"):
            entries["args"] = lambda: self.array(
                [lambda arg=arg: self.command_arg(arg) for arg in object_["args"]]
            )
        if object_.get("cwd") is not None:
            entries["cwd"] = lambda: self.value(object_["cwd"])
        if object_.get("env"):
            entries["env"] = lambda: self.map(
                {
                    key: lambda value=value: self.command_arg(value)
                    for key, value in object_["env"].items()
                }
            )
        entries["executable"] = lambda: self.command_executable(object_["executable"])
        entries["host"] = lambda: self.value(object_["host"])
        if object_.get("stdin") is not None:
            entries["stdin"] = lambda: self.blob(cast("Blob", object_["stdin"]))
        if object_.get("user") is not None:
            entries["user"] = lambda: self.value(object_["user"])
        return self.call("command", self.map(entries))

    def command_executable(self, executable: ExecutableObject) -> str:
        entries = {}
        if field(executable, "artifact") is not None:
            entries["artifact"] = lambda: self.artifact(field(executable, "artifact"))
        if field(executable, "path") is not None:
            entries["path"] = lambda: self.value(field(executable, "path"))
        return self.map(entries)

    def command_arg(self, arg: CommandValue) -> str:
        return self.map(
            {
                "kind": lambda: self.value(field(arg, "kind")),
                "value": lambda: self.value(field(arg, "value")),
            }
        )

    def error_object(self, object_: ErrorObjectValue) -> str:
        entries = {}
        if object_.get("code") is not None:
            entries["code"] = lambda: self.value(object_["code"])
        if object_.get("diagnostics") is not None:
            entries["diagnostics"] = lambda: self.array(
                [
                    lambda diagnostic=diagnostic: self.diagnostic(diagnostic)
                    for diagnostic in cast(
                        "list[DiagnosticObject]", object_["diagnostics"]
                    )
                ]
            )
        if object_.get("kind") is not None:
            entries["kind"] = lambda: self.value(object_["kind"])
        if object_.get("location") is not None:
            entries["location"] = lambda: self.error_location(
                cast("ErrorLocationObject", object_["location"])
            )
        if object_.get("message") is not None:
            entries["message"] = lambda: self.value(object_["message"])
        if object_.get("source") is not None:
            entries["source"] = lambda: self.error_source(
                cast("Referent[ErrorObjectValue | Error]", object_["source"])
            )
        if object_.get("stack") is not None:
            entries["stack"] = lambda: self.array(
                [
                    lambda location=location: self.error_location(location)
                    for location in cast("list[ErrorLocationObject]", object_["stack"])
                ]
            )
        if object_.get("values"):
            entries["values"] = lambda: self.map(
                {
                    key: lambda value=value: self.value(value)
                    for key, value in object_["values"].items()
                }
            )
        return self.call("error", self.map(entries))

    def error_source(self, source: Referent[ErrorObjectValue | Error]) -> str:
        if self.has_referent_options(field(source, "options")):
            return self.referent(source, self.error_source_node)
        return self.error_source_node(field(source, "node"))

    def error_source_node(self, node: ErrorObjectValue | Error) -> str:
        from ..error import Error

        if isinstance(node, Error):
            return self.error(node)
        return self.error_object(node)

    def error_location(self, location: ErrorLocationObject) -> str:
        entries = {
            "file": lambda: self.error_file(field(location, "file")),
            "range": lambda: self.range(field(location, "range")),
        }
        if field(location, "symbol") is not None:
            entries["symbol"] = lambda: self.value(field(location, "symbol"))
        return self.map(entries)

    def error_file(self, file: ErrorFileObject) -> str:
        return self.map(
            {
                "kind": lambda: self.value(field(file, "kind")),
                "value": lambda: (
                    self.module(field(file, "value"))
                    if field(file, "kind") == "module"
                    else self.value(field(file, "value"))
                ),
            }
        )

    def diagnostic(self, diagnostic: DiagnosticObject) -> str:
        entries = {}
        if field(diagnostic, "location") is not None:
            entries["location"] = lambda: self.module_location(
                field(diagnostic, "location")
            )
        entries["message"] = lambda: self.value(field(diagnostic, "message"))
        entries["severity"] = lambda: self.value(field(diagnostic, "severity"))
        return self.map(entries)

    def module_location(self, location: ModuleLocationObject) -> str:
        return self.map(
            {
                "module": lambda: self.module(field(location, "module")),
                "range": lambda: self.range(field(location, "range")),
            }
        )

    def range(self, range_: Range) -> str:
        return self.map(
            {
                "start": lambda: self.position(field(range_, "start")),
                "end": lambda: self.position(field(range_, "end")),
            }
        )

    def position(self, position: Position) -> str:
        return self.map(
            {
                "line": lambda: self.value(field(position, "line")),
                "character": lambda: self.value(field(position, "character")),
            }
        )

    def module(self, module: Module) -> str:
        return self.map(
            {
                "kind": lambda: self.value(module.kind),
                "referent": lambda: self.referent(
                    module.to_referent(),
                    lambda node: (
                        self.value(node)
                        if isinstance(node, str)
                        else self.graph_edge_object(node)
                    ),
                ),
            }
        )

    def referent[T](self, referent: Referent[T], node: Callable[[T], str]) -> str:
        entries = {"node": lambda: node(field(referent, "node"))}
        options = field(referent, "options")
        if self.has_referent_options(options):
            entries["options"] = lambda: self.referent_options(options)
        return self.map(entries)

    def has_referent_options(self, options: ReferentOptions | None) -> bool:
        return options is not None and any(
            field(options, key) is not None
            for key in ("artifact", "id", "location", "name", "path", "tag")
        )

    def referent_options(self, options: ReferentOptions) -> str:
        return self.map(
            {
                key: lambda key=key: self.value(field(options, key))
                for key in ("artifact", "id", "location", "name", "path", "tag")
                if field(options, key) is not None
            }
        )

    def mutation[T: ValueType](self, value: Mutation[T]) -> str:
        return self.call(
            "mutation",
            self.map(
                {
                    key: lambda child=child: self.value(cast("Printable", child))
                    for key, child in value.inner.items()
                    if key != "separator" or child is not None and child is not UNSET
                }
            ),
        )

    def placeholder(self, value: Placeholder) -> str:
        return self.call("placeholder", self.string(value.name))

    def template(self, value: Template) -> str:
        return (
            self.style("tg")
            + self.style("`", colors["green"])
            + "".join(
                self.style(self.escape_template_string(component), colors["green"])
                if isinstance(component, str)
                else self.style("${") + self.value(component) + self.style("}")
                for component in value.components
            )
            + self.style("`", colors["green"])
        )

    def escape_template_string(self, value: str) -> str:
        return value.replace("\\", "\\\\").replace("`", "\\`").replace("${", "\\${")

    def array(self, values: Sequence[Callable[[], str]]) -> str:
        if self.indentation is None:
            return (
                self.style("[")
                + self.style(",").join(value() for value in values)
                + self.style("]")
            )
        if len(values) == 0:
            return self.style("[") + self.style("]")
        return (
            self.style("[")
            + "\n"
            + self.with_indent(
                lambda: "\n".join(
                    self.indent() + value() + self.style(",") for value in values
                )
            )
            + "\n"
            + self.indent()
            + self.style("]")
        )

    def map(self, value: Mapping[str, Callable[[], str]]) -> str:
        entries = object_entries(value)
        if self.indentation is None:
            return (
                self.style("{")
                + self.style(",").join(
                    self.style(json_string(key), colors["green"])
                    + self.style(":")
                    + value()
                    for key, value in entries
                )
                + self.style("}")
            )
        if len(entries) == 0:
            return self.style("{") + self.style("}")
        return (
            self.style("{")
            + "\n"
            + self.with_indent(
                lambda: "\n".join(
                    self.indent()
                    + self.style(json_string(key), colors["green"])
                    + self.style(":")
                    + " "
                    + value()
                    + self.style(",")
                    for key, value in entries
                )
            )
            + "\n"
            + self.indent()
            + self.style("}")
        )

    def string(self, value: str) -> str:
        return self.style(json_string(value), colors["green"])

    def id(self, value: str) -> str:
        return self.style(value, colors["blue"])

    def object_id(self, state: ObjectState) -> str:
        string = Referent.with_node_and_tokens(state.id, state.tokens).to_data_string()
        id_, separator, query = string.partition("?")
        if not separator:
            return self.id(string)
        return self.id(id_) + self.style(separator + query, colors["gray"])

    def bytes_equal(
        self, a: bytes | bytearray | memoryview, b: bytes | bytearray | memoryview
    ) -> bool:
        return len(a) == len(b) and all(
            value == b[index] for index, value in enumerate(a)
        )

    def indent(self) -> str:
        return (self.indentation or "") * self.indent_

    def with_indent(self, render: Callable[[], str]) -> str:
        self.indent_ += 1
        try:
            return render()
        finally:
            self.indent_ -= 1

    def call(self, name: str, arg: str | None = None) -> str:
        return (
            self.style("tg")
            + self.style(".")
            + self.style(name, colors["blue"])
            + self.style("(")
            + (arg or "")
            + self.style(")")
        )

    def style(self, value: str, code: str | None = None) -> str:
        return (
            code + value + colors["reset"] if self.color and code is not None else value
        )


colors = {
    "reset": "\x1b[0m",
    "gray": "\x1b[38;5;244m",
    "red": "\x1b[91m",
    "cyan": "\x1b[96m",
    "magenta": "\x1b[95m",
    "yellow": "\x1b[93m",
    "green": "\x1b[32m",
    "blue": "\x1b[94m",
}


def field(value: object, key: str) -> Any:
    # The printer accepts dictionary and attribute representations at this boundary.
    return value.get(key) if isinstance(value, dict) else getattr(value, key, None)


def json_string(value: str) -> str:
    # JSON.stringify escapes lone UTF-16 surrogates and leaves Unicode scalars intact.
    string = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return "".join(
        f"\\u{ord(character):04x}" if 0xD800 <= ord(character) <= 0xDFFF else character
        for character in string
    )


def object_entries[T](value: Mapping[str, T]) -> list[tuple[str, T]]:
    # JavaScript enumerates array-index keys before other object properties.
    indices = []
    entries = []
    for key, child in value.items():
        if (
            isinstance(key, str)
            and key.isascii()
            and key.isdecimal()
            and (len(key) == 1 or not key.startswith("0"))
            and len(key) <= 10
            and int(key) < 2**32 - 1
        ):
            indices.append((key, child))
        else:
            entries.append((key, child))
    return sorted(indices, key=lambda entry: int(entry[0])) + entries


def number_string(value: int | float) -> str:
    if isinstance(value, int):
        return str(value)
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    if value == 0:
        return "0"
    string = repr(value)
    if 1e-6 <= abs(value) < 1e21:
        return (
            format(Decimal(string), "f").rstrip("0").rstrip(".")
            if "." in format(Decimal(string), "f")
            else format(Decimal(string), "f")
        )
    mantissa, exponent = string.split("e") if "e" in string else (string, "0")
    mantissa = mantissa.removesuffix(".0")
    exponent = int(exponent)
    return mantissa + "e" + ("+" if exponent >= 0 else "-") + str(abs(exponent))
