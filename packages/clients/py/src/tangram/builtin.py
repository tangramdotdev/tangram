"""Tangram builtins, corresponding to the JavaScript client's builtin.ts."""

import asyncio
from typing import TYPE_CHECKING, Literal, NotRequired, TypedDict, TypeGuard

from .blob import Blob
from .resolve import Unresolved

if TYPE_CHECKING:
    from .client import Client

from . import host, path
from .client import client as default_client
from .directory import Directory
from .file import File
from .placeholder import output
from .process.build import build
from .resolve import resolve
from .symlink import Symlink

type ArtifactValue = Directory | File | Symlink

type ArchiveFormat = Literal["tar", "zip"]
type CompressionFormat = Literal["bz2", "gz", "xz", "zst"]


class DownloadOptions(TypedDict):
    checksum: NotRequired[Literal["blake3", "sha256", "sha512"] | None]
    mode: NotRequired[Literal["raw", "decompress", "extract"] | None]


def _is_artifact(value: object) -> TypeGuard[ArtifactValue]:
    return isinstance(value, (Directory, File, Symlink))


async def archive(
    artifact: ArtifactValue,
    format: ArchiveFormat,
    compression: CompressionFormat | None = None,
    *,
    client: "Client | None" = None,
) -> Blob:
    await _validate_archive_artifact(artifact, client)
    args = [
        "builtin",
        "archive",
        *(["--compression", compression] if compression is not None else []),
        "--format",
        format,
        "--input",
        artifact,
        "--output",
        output,
    ]
    value = (
        await build(client=client)
        .host(host.current)
        .executable("tg")
        .args(args)
        .named("archive")
    )
    assert isinstance(value, File)
    return await value.contents(client)


async def _validate_archive_artifact(
    artifact: ArtifactValue, client: "Client | None"
) -> None:
    if isinstance(artifact, Directory):
        for child in (await artifact.entries(client)).values():
            await _validate_archive_artifact(child, client)
    elif isinstance(artifact, File):
        if await artifact.dependencies(client):
            raise ValueError("cannot archive a file with dependencies")
    else:
        if await artifact.artifact(client) is not None:
            raise ValueError("cannot archive a symlink with an artifact")
        if await artifact.path(client) is None:
            raise ValueError("cannot archive a symlink without a path")


async def bundle(
    artifact: Unresolved[ArtifactValue], *, client: "Client | None" = None
) -> ArtifactValue:
    artifact = await resolve(artifact)
    assert _is_artifact(artifact)
    client = client or default_client
    await artifact.store(client)
    dependencies = {}
    await _collect_dependencies(artifact, dependencies, client)
    if not dependencies:
        return artifact
    assert isinstance(artifact, Directory)
    entries = {}
    for id, dependency in sorted(dependencies.items()):
        entries[id] = await _remove_dependencies(dependency, 3, client)
    store = await Directory.new(entries, client=client)
    value = await _remove_dependencies(artifact, 0, client)
    assert isinstance(value, Directory)
    return await Directory.new(value, {".tangram": {"store": store}}, client=client)


async def compress(
    blob: Blob, format: CompressionFormat, *, client: "Client | None" = None
) -> Blob:
    input = await File.new(blob, client=client)
    args = [
        "builtin",
        "compress",
        "--format",
        format,
        "--input",
        input,
        "--output",
        output,
    ]
    value = (
        await build(client=client)
        .host(host.current)
        .executable("tg")
        .args(args)
        .named("compress")
    )
    assert isinstance(value, File)
    return await value.contents(client)


async def decompress(blob: Blob, *, client: "Client | None" = None) -> Blob:
    input = await File.new(blob, client=client)
    args = ["builtin", "decompress", "--input", input, "--output", output]
    value = (
        await build(client=client)
        .host(host.current)
        .executable("tg")
        .args(args)
        .named("decompress")
    )
    assert isinstance(value, File)
    return await value.contents(client)


async def download(
    url: str,
    checksum: str | None = None,
    options: DownloadOptions | None = None,
    *,
    client: "Client | None" = None,
) -> Blob | ArtifactValue:
    checksum = "sha512:none" if checksum is None else checksum
    options = options if options is not None else {}
    if options.get("checksum") is None:
        from .checksum import Checksum

        options["checksum"] = Checksum.algorithm(checksum)
    mode = options.get("mode")
    mode = "raw" if mode is None else mode
    args = [
        "builtin",
        "download",
        *(
            ["--checksum", options["checksum"]]
            if options.get("checksum") is not None
            else []
        ),
        "--mode",
        mode,
        "--output",
        output,
        url,
    ]
    value = (
        await build(client=client)
        .host(host.current)
        .executable("tg")
        .args(args)
        .checksum(checksum)
        .named("download")
        .network(True)
    )
    if mode == "raw":
        assert isinstance(value, File)
        return await value.contents(client)
    assert _is_artifact(value)
    return value


async def extract(blob: Blob, *, client: "Client | None" = None) -> ArtifactValue:
    input = await File.new(blob, client=client)
    args = ["builtin", "extract", "--input", input, "--output", output]
    value = (
        await build(client=client)
        .host(host.current)
        .executable("tg")
        .args(args)
        .named("extract")
    )
    assert _is_artifact(value)
    return value


async def checksum(
    input: Unresolved[str | bytes | Blob | File],
    algorithm: Literal["blake3", "sha256", "sha512"],
    *,
    client: "Client | None" = None,
) -> str:
    from .checksum import Checksum

    return await Checksum.new(input, algorithm, client=client)


async def _collect_dependencies(
    artifact: ArtifactValue, dependencies: dict[str, ArtifactValue], client: "Client"
) -> None:
    if isinstance(artifact, Directory):
        for child in (await artifact.entries(client)).values():
            await _collect_dependencies(child, dependencies, client)
    elif isinstance(artifact, File):
        for dependency in await artifact.dependency_objects(client):
            if not _is_artifact(dependency) or dependency.id in dependencies:
                continue
            dependencies[dependency.id] = dependency
            await _collect_dependencies(dependency, dependencies, client)
    else:
        dependency = await artifact.artifact(client)
        if dependency is not None and dependency.id not in dependencies:
            dependencies[dependency.id] = dependency
            await _collect_dependencies(dependency, dependencies, client)


async def _remove_dependencies(
    artifact: ArtifactValue, depth: int, client: "Client"
) -> ArtifactValue:
    if isinstance(artifact, Directory):
        entries = await artifact.entries(client)
        children = await asyncio.gather(
            *(
                _remove_dependencies(child, depth + 1, client)
                for child in entries.values()
            )
        )
        return await Directory.new(
            dict(zip(entries, children, strict=True)), client=client
        )
    if isinstance(artifact, File):
        return await File.new(
            {
                "contents": await artifact.contents(client),
                "executable": await artifact.executable(client),
            },
            client=client,
        )
    dependency = await artifact.artifact(client)
    artifact_path = await artifact.path(client)
    components = []
    if dependency is not None:
        components.extend([".."] * max(0, depth - 1))
        components.extend([".tangram", "store", dependency.id])
    if artifact_path is not None:
        components.append(artifact_path)
    assert components
    return await Symlink.new(path.join(*components), client=client)
