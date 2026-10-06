"""Exercise the standalone client against the CLI test's server."""

import asyncio
import sys

import tangram as tg


async def main():
    assert tg.host.checksum("Hello, World!", "sha256") == (
        "sha256:dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f"
    )
    assert tg.Blob("Hello").id == (
        "blb_01zby8hmr9wc7c8t2g8c7qt29cyt8hkeg2y6y1yahh585dx6hebf2g"
    )
    value = {"hello": "world", "nested": [True, 42, None], "bytes": b"\x00\xff"}
    assert tg.Value.parse(tg.Value.stringify(value)) == value
    child = await tg.host.spawn(
        "sh",
        ["-c", 'printf "Hello, World!"'],
        stdin="null",
        stdout="pipe",
        stderr="null",
    )
    assert await tg.host.read(child.stdout) == b"Hello, World!"
    assert await tg.host.read(child.stdout) is None
    assert await child.wait() == {"exit": 0}
    assert await tg.host.getxattr(sys.argv[1], "user.missing") is None
    assert await tg.host.getxattr(sys.argv[1], "user.example") == b""
    assert await tg.host.getxattr(sys.argv[1], "user.tangram.output") is None
    assert await tg.host.getxattr(sys.argv[1], "user.tangram.output.0") == b'{"PATH":'
    assert await tg.host.getxattr(sys.argv[1], "user.tangram.output.1") == b'"bin"}'
    async with tg.Client() as client:
        response = await client.send(tg.http.Request("GET", "/health"))
        assert response.status == 200
        assert (await response.json())["version"]
        # Exercise concurrent streams and flow control beyond the initial HTTP/2 window.
        data = b"Python HTTP/2\n" * 20000
        blob_id = await client.write(data)
        assert await client.read(blob_id) == data
        assert (
            await client.read(
                blob_id,
                position=13,
                length=26,
            )
            == data[13:39]
        )
        directory = tg.Directory(
            {"hello.txt": tg.File("Hello, World!"), "link": tg.Symlink("hello.txt")}
        )
        await directory.store(client)
        loaded = tg.Directory.with_referent(directory.to_referent())
        assert (
            await (await loaded.get("hello.txt", client)).text(client)
            == "Hello, World!"
        )
        assert await (await loaded.entries(client))["link"].path(client) == "hello.txt"
        assert await (await loaded.get("link", client)).text(client) == "Hello, World!"
        blobs = [tg.Blob(str(index)) for index in range(8)]
        await asyncio.gather(*(blob.store(client) for blob in blobs))
        assert await asyncio.gather(
            *(client.read(blob.id, tokens=blob.tokens) for blob in blobs)
        ) == [str(index).encode() for index in range(8)]
    print("hello from the Python client")


asyncio.run(main())
