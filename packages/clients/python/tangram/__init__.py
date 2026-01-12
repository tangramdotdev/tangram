"""Tangram Python Client.

This module provides the Python client for Tangram, mirroring the JavaScript client API.
"""

from __future__ import annotations

from tangram.assert_ import assert_, todo, unimplemented, unreachable
from tangram.blob import Blob
from tangram.build import BuildBuilder, build
from tangram.builtin import (
    archive,
    bundle,
    checksum,
    compress,
    decompress,
    download,
    extract,
)
from tangram.command import Command, CommandBuilder, command
from tangram.directory import Directory
from tangram.file import File
from tangram.handle import (
    Handle,
    PostObjectBatchArg,
    ReadArg,
    SpawnArg,
    SpawnOutput,
    get_handle,
    set_handle,
)
from tangram.mutation import Mutation, MutationKind
from tangram.object_ import Object_, ObjectState, id_kind
from tangram.placeholder import Placeholder, output, placeholder
from tangram.process import Process, ProcessContext, ProcessWait, process, set_process
from tangram.resolve import gather, resolve, store_and_serialize
from tangram.run import RunBuilder, run
from tangram.symlink import Symlink
from tangram.template import Template, template, unindent
from tangram.util import (
    Args,
    MaybeAwaitable,
    MaybeMutation,
    MutationMap,
    Resolved,
    Unresolved,
    ValueOrMaybeMutationMap,
)
from tangram import value as _value

# New type modules.
from tangram.artifact import Artifact
from tangram.artifact import with_id as artifact_with_id
from tangram.artifact import is_ as artifact_is
from tangram.artifact import expect as artifact_expect
from tangram.artifact import assert_ as artifact_assert
from tangram.checksum import Checksum
from tangram.checksum import checksum as checksum_new
from tangram.checksum import algorithm as checksum_algorithm
from tangram.checksum import is_ as checksum_is
from tangram.diagnostic import Diagnostic, DiagnosticData, Severity
from tangram.error_ import Error_ as Error
from tangram.error_ import error
from tangram.graph import Graph, graph
from tangram.location import Location, LocationData
from tangram.module_ import Module, ModuleData, Kind as ModuleKind
from tangram.position import Position
from tangram.range_ import Range
from tangram.reference import Reference
from tangram.referent import Referent, Options as ReferentOptions
from tangram.tag import Tag


# Create a Value class that wraps the value module functions to match JS API.
class Value:
    """Namespace for Value operations matching the JS API."""

    @staticmethod
    def to_data(v):
        """Convert a value to serialized data."""
        return _value.to_data(v)

    @staticmethod
    def from_data(data):
        """Deserialize data to a value."""
        return _value.from_data(data)

    @staticmethod
    def is_(v):
        """Check if a value is a valid Tangram Value."""
        return _value.is_value(v)

    @staticmethod
    def is_array(v):
        """Check if value is an array of valid Values."""
        return _value.is_array(v)

    @staticmethod
    def is_map(v):
        """Check if value is a map of string keys to Values."""
        return _value.is_map(v)

    @staticmethod
    def objects(v):
        """Extract all Object references from a value recursively."""
        return _value.objects(v)

    @staticmethod
    async def store(v):
        """Store all objects in the value."""
        return await _value.store(v)


# Re-export value types for backwards compatibility.
ValueData = _value.ValueData
to_data = _value.to_data
from_data = _value.from_data
is_value = _value.is_value
is_array = _value.is_array
is_map = _value.is_map
value_objects = _value.objects
value_store = _value.store
from tangram import encoding, path


# Convenience constructors that mirror the JS API.
async def file(*args, **kwargs) -> File:
    """Create a new file. Convenience wrapper around File.new().

    When called with a string, automatically unindents multi-line content.
    This mirrors the JavaScript tagged template literal behavior:

        # JavaScript: tg.file`hello world`
        # Python equivalent:
        await tg.file("hello world")
        await tg.file('''
            multi-line
            content
        ''')
    """
    # Handle string unindenting for first positional arg.
    if args and isinstance(args[0], str):
        args = (unindent(args[0]),) + args[1:]
    return await File.new(*args, **kwargs)


async def blob(*args) -> Blob:
    """Create a new blob. Convenience wrapper around Blob.new().

    When called with a string, automatically unindents multi-line content.
    This mirrors the JavaScript tagged template literal behavior:

        # JavaScript: tg.blob`hello world`
        # Python equivalent:
        await tg.blob("hello world")
        await tg.blob('''
            multi-line
            content
        ''')
    """
    # Handle string unindenting for first positional arg.
    if args and isinstance(args[0], str):
        args = (unindent(args[0]),) + args[1:]
    return await Blob.new(*args)


async def directory(*args) -> Directory:
    """Create a new directory. Convenience wrapper around Directory.new().

    Entries can contain awaitables which will be automatically resolved:
        await tg.directory({
            "hello": tg.file("hello"),  # tg.file() returns a coroutine
            "world": some_async_func(),
        })
    """
    return await Directory.new(*args)


async def symlink(arg=None) -> Symlink:
    """Create a new symlink. Convenience wrapper around Symlink.new().

    The artifact can be an awaitable which will be automatically resolved:
        await tg.symlink(tg.file("hello"))
    """
    return await Symlink.new(arg)


async def sleep(duration: float) -> None:
    """Sleep for the given duration in seconds."""
    handle = get_handle()
    await handle.sleep(duration)


__all__ = [
    # Artifact
    "Artifact",
    "artifact_with_id",
    "artifact_is",
    "artifact_expect",
    "artifact_assert",
    # Assert
    "assert_",
    "todo",
    "unimplemented",
    "unreachable",
    # Blob
    "Blob",
    # Build
    "BuildBuilder",
    "build",
    # Builtin
    "archive",
    "bundle",
    "checksum",
    "compress",
    "decompress",
    "download",
    "extract",
    # Checksum
    "Checksum",
    "checksum_new",
    "checksum_algorithm",
    "checksum_is",
    # Command
    "Command",
    "CommandBuilder",
    "command",
    # Diagnostic
    "Diagnostic",
    "DiagnosticData",
    "Severity",
    # Directory
    "Directory",
    # Encoding
    "encoding",
    # Error
    "Error",
    "error",
    # File
    "File",
    # Graph
    "Graph",
    "graph",
    # Handle
    "Handle",
    "get_handle",
    "set_handle",
    "ReadArg",
    "SpawnArg",
    "SpawnOutput",
    "PostObjectBatchArg",
    # Location
    "Location",
    "LocationData",
    # Module
    "Module",
    "ModuleData",
    "ModuleKind",
    # Mutation
    "Mutation",
    "MutationKind",
    # Object
    "Object_",
    "ObjectState",
    "id_kind",
    # Path
    "path",
    # Placeholder
    "Placeholder",
    "placeholder",
    "output",
    # Position
    "Position",
    # Process
    "Process",
    "ProcessContext",
    "ProcessWait",
    "process",
    "set_process",
    # Range
    "Range",
    # Reference
    "Reference",
    # Referent
    "Referent",
    "ReferentOptions",
    # Resolve
    "gather",
    "resolve",
    "store_and_serialize",
    # Run
    "RunBuilder",
    "run",
    # Symlink
    "Symlink",
    # Tag
    "Tag",
    # Template
    "Template",
    "template",
    "unindent",
    # Utility types
    "Args",
    "MaybeAwaitable",
    "MaybeMutation",
    "MutationMap",
    "Unresolved",
    "Resolved",
    "ValueOrMaybeMutationMap",
    # Value
    "Value",
    "ValueData",
    "to_data",
    "from_data",
    "is_value",
    "is_array",
    "is_map",
    "value_objects",
    "value_store",
    # Convenience functions
    "file",
    "blob",
    "directory",
    "symlink",
    "sleep",
]
