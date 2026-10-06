"""Explicit clients reach process getters and sandbox operations."""

import asyncio

import tangram as tg


async def main():
    async with (
        tg.Client() as client,
        tg.Client(url="http://127.0.0.1:1") as unavailable,
    ):
        sandbox = await tg.Sandbox.create(client=client)
        sandbox.client = unavailable
        try:
            await sandbox.load(client)
            await sandbox.reload(client)
            executable = await tg.File.new("#!/bin/sh\nexit 0\n", executable=True)
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
            output = await tg.last_output(
                await client.spawn_process(
                    {
                        "command": command,
                        "cached": False,
                        "sandbox": sandbox.id,
                        "stdin": "null",
                        "stdout": "null",
                        "stderr": "null",
                    }
                )
            )
            options = {
                "location": None
                if output.get("location") is None
                else tg.Location.Arg.from_location(output["location"]),
                "tokens": output.get("tokens") or {},
            }
            process = tg.Process(output["process"], client=unavailable, **options)
            await process.load(client)
            assert await process.command(client) is not None
            assert await process.args(client) == []
            assert await process.cwd(client) is None
            assert (await process.executable(client))["artifact"].id == executable.id
            assert await process.user(client) is None
            assert await process.sandbox(client) == sandbox.id
            assert await process.mounts(client) == []
            assert isinstance(await process.network(client), bool)
            assert await process.ports(client) == []
            assert (
                await client.wait_process(
                    output["process"], lease=output.get("lease"), **options
                )
            )["exit"] == 0
        finally:
            await sandbox.destroy(client)


asyncio.run(asyncio.wait_for(main(), 30))
