"""Process waiting, translated from client/process/wait.ts."""

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from tangram.client import Client

import json
from collections.abc import Awaitable, Callable
from typing import NotRequired, TypedDict

from tangram.error import Error, ErrorData
from tangram.http import Request, percent_encode
from tangram.location import Arg as LocationArg
from tangram.location import ArgObject as LocationArgObject
from tangram.process.outcome import Outcome, ProcessOutcome
from tangram.value import ValueType


class Wait:
    class Arg(TypedDict):
        lease: NotRequired[str | None]
        location: NotRequired[LocationArgObject | None]
        source: NotRequired[str]
        tokens: NotRequired[dict[str, list[str]] | None]


async def wait_process(
    client: "Client", id: str, arg=None, **options
) -> ProcessOutcome[ValueType]:
    promise = await wait_process_promise(client, id, arg, **options)
    outcome = await promise()
    if outcome is None:
        raise ValueError("failed to find the process")
    return outcome


async def wait_process_promise(
    client: "Client", id: str, arg=None, **options
) -> Callable[[], Awaitable[ProcessOutcome[ValueType]]]:
    promise = await try_wait_process_promise(client, id, arg, **options)
    if promise is None:
        raise ValueError("failed to find the process")
    return promise


async def try_wait_process_promise(
    client: "Client", id: str, arg=None, **options
) -> Callable[[], Awaitable[ProcessOutcome[ValueType]]] | None:
    arg = {**(arg or {}), **options}

    async def wait():
        return await wait_process_loop(client, id, arg)

    return wait


async def wait_process_loop(client: "Client", id, arg):
    while True:
        outcome = await wait_process_once(client, id, arg)
        if outcome is not None:
            return outcome


async def wait_process_once(client: "Client", id, arg):
    request = Request(
        "POST",
        f"/processes/{percent_encode(id)}/wait",
        {"accept": "text/event-stream"},
    ).arg(
        {
            **arg,
            "location": None
            if arg.get("location") is None
            else LocationArg.to_data_string(arg["location"]),
            "tokens": {} if arg.get("tokens") is None else arg["tokens"],
        }
    )
    response = await client.send_with_retry(request)
    if response.status == 404:
        await response.close()
        raise ValueError("failed to find the process")
    elif response.status < 200 or response.status >= 300:
        raise Error.from_data(cast(ErrorData, await response.json()))
    outcome = None
    async for event in response.sse():
        if event.get("event") == "outcome":
            outcome = Outcome.from_data(json.loads(event["data"]))
        elif event.get("event") == "error":
            data = json.loads(event["data"])
            raise (
                Error.with_id(data) if isinstance(data, str) else Error.from_data(data)
            )
        else:
            raise ValueError("invalid process wait event")
    return outcome
