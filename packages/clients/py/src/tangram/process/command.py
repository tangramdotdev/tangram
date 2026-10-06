"""Inherit and remove proofs on inline process command data."""

from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING, NotRequired, TypedDict

if TYPE_CHECKING:
    from ..command import CommandValueData

from ..authorization import Tokens
from ..module import Module
from ..referent import Referent
from ..value import Value


class ProcessCommandData(TypedDict):
    args: NotRequired[list[CommandValueData]]
    cwd: NotRequired[str | None]
    env: NotRequired[dict[str, CommandValueData]]
    executable: str | dict[str, object]
    host: str
    stdin: NotRequired[str | dict[str, object] | None]
    user: NotRequired[str | None]


def inherit_options(command: ProcessCommandData, options) -> ProcessCommandData:
    output = deepcopy(command)
    output["executable"] = inherit_referent(output["executable"], options)
    if output.get("stdin") is not None:
        output["stdin"] = inherit_referent(output["stdin"], options)
    for value in [*(output.get("args") or []), *(output.get("env") or {}).values()]:
        inherit_value(value["value"], options)
    return output


def without_location_and_tokens(command):
    return {
        **command,
        "args": [
            {**value, "value": Value.Data.without_location_and_tokens(value["value"])}
            for value in command.get("args") or []
        ],
        "env": {
            key: {
                **value,
                "value": Value.Data.without_location_and_tokens(value["value"]),
            }
            for key, value in (command.get("env") or {}).items()
        },
        "executable": Referent.from_data(command["executable"])
        .without_location_and_tokens()
        .to_data(),
        "stdin": None
        if command.get("stdin") is None
        else (
            Referent.from_data_string(command["stdin"])
            if isinstance(command["stdin"], str)
            else Referent.from_data(command["stdin"])
        )
        .without_location_and_tokens()
        .to_data(),
    }


def inherit_referent(data, options, resource=None):
    referent = (
        Referent.from_data_string(data)
        if isinstance(data, str)
        else Referent.from_data(data)
    )
    if referent.options is None:
        referent.options = {}
    if referent.options.get("location") is None:
        referent.options["location"] = options.get("location")
    if referent.options.get("tokens") is None:
        referent.options["tokens"] = {}
    if resource is None:
        if isinstance(referent.node, str):
            resource = referent.node
        elif isinstance(referent.node, dict) and isinstance(
            referent.node.get("artifact"), str
        ):
            resource = referent.node["artifact"]
    tokens = referent.options["tokens"]
    if tokens is None:
        tokens = {}
        referent.options["tokens"] = tokens
    Tokens.inherit(tokens, options.get("tokens") or {}, resource)
    return referent.to_data()


def inherit_string(data, options):
    referent = Referent.from_data_string(data)
    if referent.options is None:
        referent.options = {}
    if referent.options.get("location") is None:
        referent.options["location"] = options.get("location")
    if referent.options.get("tokens") is None:
        referent.options["tokens"] = {}
    tokens = referent.options["tokens"]
    if tokens is None:
        tokens = {}
        referent.options["tokens"] = tokens
    Tokens.inherit(tokens, options.get("tokens") or {}, referent.node)
    return referent.to_data_string()


def inherit_value(value, options):
    if isinstance(value, list):
        for child in value:
            inherit_value(child, options)
    elif isinstance(value, dict):
        kind = value.get("kind")
        if kind == "map":
            for child in value["value"].values():
                inherit_value(child, options)
        elif kind == "module":
            module = Module.from_data(value["value"])
            children = module.children()
            resource = children[0].id if children else None
            value["value"]["referent"] = inherit_referent(
                value["value"]["referent"], options, resource
            )
        elif kind == "object":
            value["value"] = inherit_string(value["value"], options)
        elif kind == "template":
            inherit_template(value["value"], options)
        elif kind == "mutation":
            mutation = value["value"]
            kind = mutation["kind"]
            if kind in ("append", "prepend"):
                for child in mutation["values"]:
                    inherit_value(child, options)
            elif kind == "merge":
                for child in mutation["value"].values():
                    inherit_value(child, options)
            elif kind in ("set", "set_if_unset"):
                inherit_value(mutation["value"], options)
            elif kind in ("prefix", "suffix"):
                inherit_template(mutation["template"], options)


def inherit_template(template, options):
    for component in template["components"]:
        if component["kind"] == "artifact":
            component["value"] = inherit_string(component["value"], options)
