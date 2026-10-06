"""Artifact checkout arguments and progress, translated from checkout.ts."""

from typing import TYPE_CHECKING, TypedDict, Unpack

if TYPE_CHECKING:
    from tangram.client import Client

from typing import Any, Literal, NotRequired, cast

from ..http import Stream
from ..object import Object
from ..progress import Event
from ..referent import Referent


class KeywordOptions(TypedDict, total=False):
    dependencies: bool
    extension: str | None
    force: bool
    lock: Literal["auto", "attr", "file"] | None
    path: str | None


class ArgObject(TypedDict):
    nodes: list[Referent[str] | str | Object]
    dependencies: NotRequired[bool]
    extension: NotRequired[str | None]
    force: NotRequired[bool]
    lock: NotRequired[Literal["auto", "attr", "file"] | None]
    path: NotRequired[str | None]


class Arg:
    @staticmethod
    def to_json(arg: ArgObject) -> dict[str, Any]:
        output = {"nodes": [node_to_data_string(node) for node in arg["nodes"]]}
        if "dependencies" in arg and not arg["dependencies"]:
            output["dependencies"] = arg["dependencies"]
        if "extension" in arg:
            output["extension"] = arg["extension"]
        if arg.get("force"):
            output["force"] = arg["force"]
        if "lock" in arg:
            if arg["lock"] is None:
                output["lock"] = None
            elif arg["lock"] != "auto":
                output["lock"] = arg["lock"]
        if "path" in arg:
            output["path"] = arg["path"]
        return output


class Output(TypedDict):
    paths: list[str]


class Checkout:
    Arg = Arg
    Output = Output


async def checkout(
    client: "Client",
    nodes: ArgObject | list[Referent[str] | str | Object],
    **options: Unpack[KeywordOptions],
) -> Stream[Event[Output]]:
    arg = (
        {**nodes, **options} if isinstance(nodes, dict) else {"nodes": nodes, **options}
    )
    return await client._progress(
        "/checkout",
        Arg.to_json(cast(ArgObject, arg)),
        lambda value: cast(Output, value),
    )


def node_to_data_string(node):
    if isinstance(node, Object):
        node = node.to_referent()
    if isinstance(node, Referent):
        return node.to_data_string()
    if isinstance(node, dict):
        return Referent.from_data(node).to_data_string()
    return node
