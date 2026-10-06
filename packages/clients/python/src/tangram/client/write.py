"""Write a blob through the HTTP client."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tangram.client import Client

from collections.abc import AsyncIterable, AsyncIterator
from typing import TypedDict, cast, overload

from tangram.assert_ import assert_
from tangram.client import check_response
from tangram.http import Body, Request
from tangram.referent import Referent


class Write:
    class Arg(TypedDict, total=False):
        checkoutPointers: bool

    class Output(TypedDict):
        blob: Referent[str]


@overload
async def write(client: "Client", arg_or_bytes: str | bytes) -> str: ...


@overload
async def write(
    client: "Client", arg_or_bytes: Write.Arg, input: AsyncIterable[bytes]
) -> Write.Output: ...


async def write(
    client: "Client",
    arg_or_bytes: Write.Arg | str | bytes,
    input: AsyncIterable[bytes] | None = None,
) -> Write.Output | str:
    if isinstance(arg_or_bytes, (str, bytes)):
        bytes_ = (
            arg_or_bytes.encode("utf-8")
            if isinstance(arg_or_bytes, str)
            else arg_or_bytes
        )
        output = await write(client, {}, single_bytes(bytes_))
        return output["blob"].node
    method = "POST"
    uri = "/write"
    headers = {
        "accept": "application/json",
        "content-type": "application/octet-stream",
    }
    assert_(input is not None)
    body = Body(cast(AsyncIterable[bytes], input))
    request = Request(method, uri, headers, body).arg(
        {"checkout_pointers": arg_or_bytes.get("checkoutPointers")}
    )
    response = await client.send(request)
    await check_response(response)
    output = cast(dict[str, str], await response.json())
    return {"blob": Referent.from_data_string(output["blob"])}


async def single_bytes(bytes_: bytes) -> AsyncIterator[bytes]:
    yield bytes_
