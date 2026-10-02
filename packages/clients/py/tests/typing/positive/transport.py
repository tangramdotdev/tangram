from collections.abc import AsyncIterator
from typing import assert_type

from tangram.client import Client
from tangram.client.process.cancel import Cancel
from tangram.client.process.get import Get
from tangram.client.write import Write
from tangram.http import Body, Response
from tangram.http.body import SseEvent


async def consume(client: Client, chunks: AsyncIterator[bytes]) -> None:
    assert_type(await client.write("hello"), str)
    assert_type(await client.write({}, chunks), Write.Output)
    assert_type(await client.read("blob-id"), bytes)
    assert_type(await client.try_read("blob-id"), bytes | None)
    assert_type(await client.get_process("process-id"), Get.Output)
    assert_type(
        await client.cancel_process("process-id", {"lease": "lease"}), Cancel.Output
    )
    assert_type(
        await client.try_signal_process("process-id", {"signal": "sigterm"}),
        bool | None,
    )
    assert_type(Body.json({"hello": "world"}), Body)
    assert_type(await Body.json({"hello": "world"}).json(), object)
    assert_type(await Response(200).json(), object)
    assert_type(Body.text("hello"), Body)
    assert_type(await Body.text("hello").collect(), bytes)
    assert_type(Body.empty().sse(), AsyncIterator[SseEvent])
    assert_type(Response(200).sse(), AsyncIterator[SseEvent])


async def utility_types() -> None:
    from tangram.args import Args
    from tangram.authorization import Tokens
    from tangram.blob import Blob
    from tangram.builtin import archive, compress
    from tangram.directory import Directory

    def mapped(value: int) -> dict[str, object]:
        return {"count": value}

    assert_type(
        await Args.apply([1, 2], map=mapped, reduce={"count": "set"}), dict[str, object]
    )
    assert_type(Tokens.normalize({"local": ["token"]}), None)
    assert_type(await archive(Directory({}), "tar"), Blob)
    assert_type(await compress(Blob("bytes"), "gz"), Blob)
