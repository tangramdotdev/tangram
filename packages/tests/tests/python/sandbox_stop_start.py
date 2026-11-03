"""A sandbox handle stops and starts its sandbox."""

import asyncio

import tangram as tg


async def main():
    async with tg.Client() as client:
        sandbox = await tg.Sandbox.create(client=client)
        try:
            await sandbox.stop(client)
            await sandbox.reload(client)
            assert sandbox.data["status"] == "stopped"
            await sandbox.start(client)
            await sandbox.reload(client)
            assert sandbox.data["status"] == "started"
        finally:
            await sandbox.destroy(client)


asyncio.run(asyncio.wait_for(main(), 30))
