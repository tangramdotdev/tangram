from collections.abc import Awaitable
from typing import assert_type

from tangram.blob import Blob, BlobBuilder, BlobChildInput, BlobDataLeaf, blob
from tangram.directory import Directory, DirectoryBuilder, directory
from tangram.file import File, FileBuilder, file
from tangram.graph import Graph, GraphBuilder
from tangram.sandbox import Builder as SandboxBuilder
from tangram.sandbox import Sandbox
from tangram.symlink import Symlink, SymlinkBuilder, symlink


async def check(blob_future: Awaitable[Blob], path_future: Awaitable[str]) -> None:
    child: BlobChildInput = {"blob": blob_future, "length": 4}
    assert_type(blob({"children": [child]}), BlobBuilder)
    assert_type(await blob("contents"), Blob)
    assert_type(await Blob.new(blob_future), Blob)
    assert_type(await Blob("contents").bytes, bytes)
    assert_type(await Blob("contents").length, int)
    assert_type(await Blob("contents").text, str)
    assert_type(directory().entry("foo", blob_future), DirectoryBuilder)
    assert_type(await directory(), Directory)
    assert_type(file().contents(blob_future).executable(True), FileBuilder)
    assert_type(await file("contents"), File)
    assert_type(await File("contents").contents, Blob)
    assert_type(await File("contents").module, str | None)
    assert_type(symlink().path(path_future), SymlinkBuilder)
    assert_type(await symlink("target"), Symlink)
    assert_type(await Symlink("target").path, str | None)
    assert_type(await GraphBuilder(), Graph)
    assert_type(Sandbox.create().cpu(1).memory(1024).network(True), SandboxBuilder)
    assert_type(await Sandbox.create(), Sandbox)
    data: BlobDataLeaf = {"bytes": "aGVsbG8="}
    assert_type(Blob.Data.children(data), list[str])
