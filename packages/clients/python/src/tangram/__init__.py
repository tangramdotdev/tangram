"""Tangram's standalone Python client."""

from __future__ import annotations

# Initialize the decorator before importing modules that use it.
from .property import property as property

# isort: split

from . import encoding, host, http, path
from . import process as process
from .args import Args
from .artifact import Artifact
from .assert_ import assert_, todo, unimplemented, unreachable
from .authorization import Authorization
from .blob import Blob, blob
from .builtin import (
    ArchiveFormat,
    CompressionFormat,
    archive,
    bundle,
    compress,
    decompress,
    download,
    extract,
)
from .checksum import Checksum, checksum
from .client import Client, client, last_output
from .client.checkin import Checkin
from .client.checkout import Checkout
from .client.process.signal import Signal
from .client.read import Read
from .client.write import Write
from .command import Command, command
from .config import Config as Config
from .diagnostic import Diagnostic
from .directory import Directory, directory
from .encoding import Encoding, set_encoding
from .error import Error, error
from .file import File, file
from .graph import Graph, Pointer, graph
from .host import Host, set_host
from .http import Body, Headers, Request, Response, Uri
from .location import Location
from .module import Module
from .mutation import Mutation, mutation
from .object import Object
from .placeholder import Placeholder, output, placeholder
from .position import Position
from .process import Builder as ProcessBuilder
from .process import Process, set_process
from .progress import Progress
from .queue import Queue
from .range import Range
from .reference import Reference
from .referent import Referent
from .resolve import Resolve, Resolved, Unresolved, resolve
from .sandbox import Sandbox
from .sleep import sleep
from .stop import Stop
from .symlink import Symlink, symlink
from .sync import Sync
from .tag import Tag
from .template import Template, template
from .util import (
    Function,
    MaybeMutation,
    MaybeMutationMap,
    MaybePromise,
    MaybeReferent,
    MutationMap,
    ResolvedArgs,
    ResolvedReturnValue,
    ReturnValue,
    UnresolvedArgs,
    ValueOrMaybeMutationMap,
)
from .value import Value

__all__ = [
    "ArchiveFormat",
    "Args",
    "Artifact",
    "Authorization",
    "Blob",
    "Body",
    "Checkin",
    "Checkout",
    "Checksum",
    "Client",
    "Config",
    "Command",
    "CompressionFormat",
    "Diagnostic",
    "Directory",
    "Encoding",
    "Error",
    "File",
    "Function",
    "Graph",
    "Headers",
    "Host",
    "Location",
    "MaybeMutation",
    "MaybeMutationMap",
    "MaybePromise",
    "MaybeReferent",
    "Module",
    "Mutation",
    "MutationMap",
    "Object",
    "Placeholder",
    "Pointer",
    "Position",
    "Process",
    "Progress",
    "Queue",
    "Range",
    "Read",
    "Reference",
    "Referent",
    "Request",
    "Resolve",
    "Resolved",
    "ResolvedArgs",
    "ResolvedReturnValue",
    "Response",
    "ReturnValue",
    "Sandbox",
    "Signal",
    "Stop",
    "Symlink",
    "Sync",
    "Tag",
    "Template",
    "Unresolved",
    "UnresolvedArgs",
    "Uri",
    "Value",
    "ValueOrMaybeMutationMap",
    "Write",
    "archive",
    "assert_",
    "blob",
    "build",
    "bundle",
    "checksum",
    "client",
    "command",
    "compress",
    "decompress",
    "directory",
    "download",
    "encoding",
    "error",
    "exec",
    "extract",
    "file",
    "graph",
    "host",
    "http",
    "last_output",
    "mutation",
    "output",
    "path",
    "placeholder",
    "process",
    "property",
    "resolve",
    "run",
    "set_encoding",
    "set_host",
    "set_process",
    "sleep",
    "spawn",
    "symlink",
    "template",
    "todo",
    "unimplemented",
    "unreachable",
]

build = Process.build
exec = Process.exec
run = Process.run
spawn = Process.spawn

Process.Builder = ProcessBuilder
