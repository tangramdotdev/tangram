from tangram.client import Client
from tangram.http import Body, Request, Response


async def invalid(client: Client) -> None:
    await client.write(42)  # error: invalid-argument-type
    Body.bytes("hello")  # error: invalid-argument-type
    Request(42, "/")  # error: invalid-argument-type
    decoded = await Response(200).json()
    decoded["field"]  # error: not-subscriptable


async def invalid_utilities() -> None:
    from tangram.authorization import Tokens
    from tangram.builtin import archive
    from tangram.directory import Directory

    await archive(Directory({}), "rar")  # error: invalid-argument-type
    Tokens.normalize({"local": "token"})  # error: invalid-argument-type
