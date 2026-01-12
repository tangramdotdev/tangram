"""Builtin functions matching JS builtin.ts."""

from typing import Any, Literal, Optional, TypedDict, Union

ArchiveFormat = Literal["tar", "tgar", "zip"]
CompressionFormat = Literal["bz2", "gz", "xz", "zst"]
ChecksumAlgorithm = Literal["blake3", "sha256", "sha512"]


class DownloadOptions(TypedDict, total=False):
    """Options for the download function."""

    checksum: Optional[ChecksumAlgorithm]
    mode: Optional[Literal["raw", "decompress", "extract"]]


async def archive(
    artifact: Any,
    format: ArchiveFormat,
    compression: Optional[CompressionFormat] = None,
) -> Any:
    """Create an archive from an artifact.

    Args:
        artifact: The artifact to archive.
        format: The archive format (tar, tgar, zip).
        compression: Optional compression format.

    Returns:
        A blob containing the archive.
    """
    from tangram.build import build
    from tangram.blob import Blob

    value = await build(
        host="builtin",
        executable="archive",
        args=[artifact, format, compression],
    )
    assert isinstance(value, Blob)
    return value


async def bundle(artifact: Any) -> Any:
    """Bundle an artifact.

    Args:
        artifact: The artifact to bundle.

    Returns:
        The bundled artifact.
    """
    from tangram.build import build

    value = await build(
        host="builtin",
        executable="bundle",
        args=[artifact],
    )
    return value


async def checksum(
    input_: Any,
    algorithm: ChecksumAlgorithm,
) -> str:
    """Compute a checksum.

    Args:
        input_: The blob or artifact to checksum.
        algorithm: The checksum algorithm.

    Returns:
        The checksum string.
    """
    from tangram.build import build

    value = await build(
        host="builtin",
        executable="checksum",
        args=[input_, algorithm],
    )
    assert isinstance(value, str)
    return value


async def compress(
    blob: Any,
    format: CompressionFormat,
) -> Any:
    """Compress a blob.

    Args:
        blob: The blob to compress.
        format: The compression format.

    Returns:
        The compressed blob.
    """
    from tangram.build import build
    from tangram.blob import Blob

    value = await build(
        host="builtin",
        executable="compress",
        args=[blob, format],
    )
    assert isinstance(value, Blob)
    return value


async def decompress(blob: Any) -> Any:
    """Decompress a blob.

    Args:
        blob: The blob to decompress.

    Returns:
        The decompressed blob.
    """
    from tangram.build import build
    from tangram.blob import Blob

    value = await build(
        host="builtin",
        executable="decompress",
        args=[blob],
    )
    assert isinstance(value, Blob)
    return value


async def download(
    url: str,
    checksum_value: str,
    options: Optional[DownloadOptions] = None,
) -> Any:
    """Download a file from a URL.

    Args:
        url: The URL to download from.
        checksum_value: The expected checksum of the file.
        options: Optional download options.

    Returns:
        The downloaded blob or artifact.
    """
    from tangram.build import build

    if options is None:
        options = {}

    value = await build(
        host="builtin",
        executable="download",
        args=[url, options],
        checksum=checksum_value,
    )
    return value


async def extract(blob: Any) -> Any:
    """Extract an archive.

    Args:
        blob: The archive blob to extract.

    Returns:
        The extracted artifact.
    """
    from tangram.build import build

    value = await build(
        host="builtin",
        executable="extract",
        args=[blob],
    )
    return value
