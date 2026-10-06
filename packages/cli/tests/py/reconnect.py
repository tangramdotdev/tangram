"""Reconnect detached process stdio and wait over replacement HTTP/2 sessions."""

import asyncio

import tangram as tg


async def main():
    async with tg.Client() as client:
        executable = await tg.file(
            "#!/bin/sh\nwhile IFS= read -r line; do printf '%s\\n' \"$line\"; done\n",
            {"executable": True},
        )
        await executable.store(client)
        command = tg.Referent(
            {
                "executable": {
                    "node": {"artifact": executable.id},
                    "options": {"tokens": executable.tokens},
                },
                "host": tg.host.current,
            }
        )
        spawned = await tg.Process.connect(
            {
                "command": command,
                "stdin": "pipe",
                "stdout": "pipe",
                "stderr": "null",
                "cached": False,
                "sandbox": {},
            },
            client=client,
            mode="spawn",
        )
        await spawned.detach()
        process = await tg.Process.connect(
            spawned.id,
            client=client,
            location=spawned.location,
            tokens=spawned.tokens,
            reads=["stdout"],
        )
        async with process:
            id, lease = process.id, process.lease
            assert lease is None
            prefix = b"before reconnect\n" * 8192
            suffix = b"after reconnect\n" * 8192
            ready = asyncio.Event()
            chunks = []
            length = 0

            async def receive():
                nonlocal length
                async for chunk in process.read():
                    chunks.append(chunk)
                    length += len(chunk)
                    if length >= len(prefix):
                        ready.set()

            reading = asyncio.create_task(receive())
            waiting = asyncio.create_task(process.wait())
            await process.write(prefix)
            await ready.wait()
            first_transport = client._session
            assert first_transport is not None
            await first_transport.close()
            # Read and wait reconnect concurrently with writes to the detached process.
            await process.write(suffix)
            await process.end()
            await reading
            result = await waiting
            assert result["exit"] == 0
            assert b"".join(chunks) == prefix + suffix
            assert process.id == id
            assert process.lease == lease
            assert client._session is not first_transport
            assert process.connection.session.output["process"] == id
            assert process.connection.session.output.get("lease") == lease
    print("process reconnected without losing or repeating stdio")


asyncio.run(asyncio.wait_for(main(), 120))
