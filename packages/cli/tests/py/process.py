"""Test concurrent stdin and stdout over a process control connection."""

import asyncio

import tangram as tg


async def main():
    async with tg.Client() as client:
        executable = tg.File(
            "#!/bin/sh\nwhile IFS= read -r line; do printf '%s\\n' \"$line\"; done\n",
            executable=True,
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
        process = await tg.Process.connect(
            {
                "command": command,
                "stdin": "pipe",
                "stdout": "pipe",
                "stderr": "null",
                "cached": False,
                "sandbox": {},
            },
            client=client,
            reads=["stdout"],
        )
        async with process:

            async def receive():
                return b"".join([bytes_ async for bytes_ in process.read()])

            reader = asyncio.create_task(receive())
            data = b"duplex python\n" * 200000
            await process.write(data)
            await process.end()
            assert await reader == data
            assert (await process.wait())["exit"] == 0
        # The separate stdio endpoints use the same receipt and cursor protocol.
        output = await tg.last_output(
            await client.spawn_process(
                {
                    "command": command,
                    "stdin": "pipe",
                    "stdout": "pipe",
                    "stderr": "null",
                    "cached": False,
                    "sandbox": {},
                }
            )
        )
        options = {
            "location": None
            if output.get("location") is None
            else tg.Location.Arg.from_location(output["location"]),
            "tokens": output.get("tokens") or {},
        }
        chunks = await client.try_read_process_stdio(
            output["process"], streams=["stdout"], **options
        )

        async def receive_stdio():
            return b"".join([chunk["bytes"] async for chunk in chunks])

        async def input_chunks():
            for position in range(0, len(data), 32768):
                yield {
                    "bytes": data[position : position + 32768],
                    "combined_position": position,
                    "stream": "stdin",
                    "stream_position": position,
                }

        reader = asyncio.create_task(receive_stdio())
        await client.write_process_stdio(
            output["process"], input_chunks(), streams=["stdin"], **options
        )
        assert await reader == data
        assert (
            await client.wait_process(
                output["process"], lease=output.get("lease"), **options
            )
        )["exit"] == 0
    print("duplex process completed")


asyncio.run(asyncio.wait_for(main(), 60))
