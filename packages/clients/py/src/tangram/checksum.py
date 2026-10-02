"""Checksum construction and the public checksum string helpers."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Literal, TypeGuard, cast

from . import host
from .assert_ import assert_
from .resolve import Unresolved

if TYPE_CHECKING:
    from .blob import Blob
    from .client import Client
    from .file import File

type ChecksumAlgorithm = Literal["blake3", "sha256", "sha512"]
type Algorithm = ChecksumAlgorithm
type ChecksumString = str
type ChecksumInput = Unresolved[str | bytes | Blob | File]


async def checksum(
    input: ChecksumInput, algorithm: Algorithm, *, client: Client | None = None
) -> ChecksumString:
    return await Checksum.new(input, algorithm, client=client)


class Checksum:
    type Algorithm = ChecksumAlgorithm

    @staticmethod
    async def new(
        input: ChecksumInput, algorithm: Algorithm, *, client: Client | None = None
    ) -> ChecksumString:
        from .blob import Blob
        from .file import File
        from .placeholder import output
        from .process.build import build
        from .resolve import resolve

        resolved_input = await resolve(input)
        if isinstance(resolved_input, (str, bytes)):
            return host.checksum(resolved_input, algorithm)
        else:
            file = (
                await File.new(resolved_input, client=client)
                if isinstance(resolved_input, Blob)
                else resolved_input
            )
            args = [
                "builtin",
                "checksum",
                "--algorithm",
                algorithm,
                "--input",
                file,
                "--output",
                output,
            ]
            value = await build(
                args=args,
                executable="tg",
                host=host.current,
                client=client,
            )
            assert_(isinstance(value, File))
            checksum = await cast(File, value).text(client)
            assert_(Checksum.is_(checksum))
            return checksum

    @staticmethod
    def algorithm(checksum: ChecksumString) -> Algorithm:
        if ":" in checksum:
            return cast(Algorithm, checksum.split(":", 1)[0])
        if "-" in checksum:
            return cast(Algorithm, checksum.split("-", 1)[0])
        raise ValueError("invalid checksum")

    @staticmethod
    def is_(value: object) -> TypeGuard[ChecksumString]:
        return (
            isinstance(value, str)
            and re.search(r"^(blake3|sha256|sha512)[-:][a-zA-Z0-9+/]+=*$", value)
            is not None
        )

    @staticmethod
    def expect(value: object) -> ChecksumString:
        assert_(Checksum.is_(value))
        return cast(ChecksumString, value)

    @staticmethod
    def assert_(value: object) -> None:
        assert_(Checksum.is_(value))
