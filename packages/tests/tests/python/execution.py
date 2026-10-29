"""Exercise the public execution builders and builtin commands against a server."""

import asyncio

import tangram as tg


async def main():
    async with tg.Client() as client:
        executable = tg.File(
            '#!/bin/sh\nprintf hello > "$TANGRAM_OUTPUT"\n', executable=True
        )
        output = await tg.build(executable=executable, client=client)
        assert isinstance(output, tg.File)
        assert await output.text(client) == "hello"
        local = await tg.run(executable=executable, client=client)
        assert await local.text(client) == "hello"
        directory = await tg.directory({"hello": "hello", "link": tg.Symlink("hello")})
        archive = await tg.archive(directory, "tar", client=client)
        extracted = await tg.extract(archive, client=client)
        assert await (await extracted.get("link", client)).text(client) == "hello"
        compressed = await tg.compress(tg.Blob("hello"), "gz", client=client)
        decompressed = await tg.decompress(compressed, client=client)
        assert await decompressed.text(client) == "hello"
        assert await tg.checksum(
            decompressed, "sha256", client=client
        ) == tg.host.checksum("hello")
        dependency = tg.File("dependency")
        bundled = await tg.bundle(
            await tg.directory({"link": tg.Symlink(artifact=dependency)}), client=client
        )
        assert await (await bundled.get("link", client)).text(client) == "dependency"
    print("python execution completed")


asyncio.run(asyncio.wait_for(main(), 120))
