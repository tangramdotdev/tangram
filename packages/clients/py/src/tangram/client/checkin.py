"""Artifact checkin arguments and progress, translated from checkin.ts."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tangram.client import Client

from typing import Any, Literal, TypedDict

from ..http import Stream
from ..http.uri import primitive_string
from ..progress import Event
from ..referent import Referent


class Options(TypedDict, total=False):
    checkout_pointers: bool
    destructive: bool
    deterministic: bool
    root: bool
    ignore: bool
    local_dependencies: bool
    lock: Literal["auto", "attr", "file"] | None
    locked: bool
    solve: bool
    unsolved_dependencies: bool
    ttl: float | None
    watch: bool


class Output(TypedDict):
    artifact: Referent[str]


class ArgObject(TypedDict):
    options: Options
    path: str
    updates: list[str]


class Arg:
    @staticmethod
    def to_json(arg: ArgObject) -> dict[str, Any]:
        source = arg["options"]
        options = {}
        checkout_pointers = source.get(
            "checkout_pointers", source.get("checkoutPointers")
        )
        if (
            "checkout_pointers" in source or "checkoutPointers" in source
        ) and not checkout_pointers:
            options["checkout_pointers"] = checkout_pointers
        if source.get("destructive"):
            options["destructive"] = source["destructive"]
        if source.get("deterministic"):
            options["deterministic"] = source["deterministic"]
        if "ignore" in source and not source["ignore"]:
            options["ignore"] = source["ignore"]
        if "lock" in source:
            if source["lock"] is None:
                options["lock"] = None
            elif source["lock"] != "auto":
                options["lock"] = source["lock"]
        if source.get("locked"):
            options["locked"] = source["locked"]
        if source.get("root"):
            options["root"] = source["root"]
        if "solve" in source and not source["solve"]:
            options["solve"] = source["solve"]
        local_dependencies = source.get(
            "local_dependencies", source.get("localDependencies")
        )
        if (
            "local_dependencies" in source or "localDependencies" in source
        ) and not local_dependencies:
            options["source_dependencies"] = local_dependencies
        if "ttl" in source:
            ttl = source["ttl"]
            options["tag_ttl"] = (
                "infinite" if ttl is None else primitive_string(ttl) + "s"
            )
        unsolved_dependencies = source.get(
            "unsolved_dependencies", source.get("unsolvedDependencies")
        )
        if unsolved_dependencies:
            options["unsolved_dependencies"] = unsolved_dependencies
        if source.get("watch"):
            options["watch"] = source["watch"]
        output = {"options": options, "path": arg["path"]}
        if arg["updates"]:
            output["updates"] = ",".join(arg["updates"])
        return output


class Checkin:
    Arg = Arg
    Output = Output
    Options = Options


async def checkin(
    client: "Client",
    arg: ArgObject | str,
    *,
    options: Options | None = None,
    updates: list[str] | None = None,
) -> Stream[Event[Output]]:
    def convert(value: dict[str, Any]) -> Output:
        return {"artifact": Referent.from_data_string(value["artifact"])}

    if isinstance(arg, str):
        arg = {"options": options or {}, "path": arg, "updates": updates or []}
    return await client._progress("/checkin", Arg.to_json(arg), convert)
