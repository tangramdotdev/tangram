"""Module type and utilities.

This module mirrors the JavaScript packages/clients/js/src/module.ts.
Note: Named module_ to avoid conflict with Python's stdlib module.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, TypedDict, Union
from urllib.parse import quote, unquote

from tangram.referent import DataOptions, Options, Referent

if TYPE_CHECKING:
    from tangram.object_ import Object_

# Module kinds.
Kind = Literal[
    "js",
    "ts",
    "dts",
    "object",
    "artifact",
    "blob",
    "directory",
    "file",
    "symlink",
    "graph",
    "command",
]

# Module item can be a path string or a graph edge.
# For now, we use Any for the graph edge since Graph is not yet implemented.
Item = Union[str, Any]


class Module(TypedDict):
    """A module with kind and referent."""

    kind: Kind
    referent: Referent[Item]


class ModuleData(TypedDict):
    """Serialized module data."""

    kind: Kind
    referent: Any  # Referent.Data


def to_data(value: Module) -> ModuleData:
    """Convert a module to data."""
    from tangram.referent import to_data as referent_to_data

    def item_to_data(item: Item) -> Any:
        if isinstance(item, str):
            return item
        # For graph edges, just return the item for now.
        # This will be expanded when Graph is implemented.
        if hasattr(item, "id"):
            return item.id
        return item

    return {
        "kind": value["kind"],
        "referent": referent_to_data(value["referent"], item_to_data),
    }


def from_data(data: ModuleData) -> Module:
    """Convert data to a module."""
    from tangram.object_ import Object_
    from tangram.referent import from_data as referent_from_data

    def item_from_data(item: Any) -> Item:
        if isinstance(item, str):
            # Check if it is a path (starts with . or /).
            if item.startswith(".") or item.startswith("/"):
                return item
            # Otherwise treat as object ID.
            return Object_.with_id(item)
        return item

    return {
        "kind": data["kind"],
        "referent": referent_from_data(data["referent"], item_from_data),
    }


def to_data_string(value: Module) -> str:
    """Convert a module to a string."""
    item = value["referent"].item
    if isinstance(item, str) and (item.startswith(".") or item.startswith("/")):
        string = item
    elif hasattr(item, "id"):
        string = item.id
    else:
        string = str(item)

    params = []
    options = value["referent"].options
    if options.get("artifact") is not None:
        params.append(f"artifact={quote(options['artifact'])}")
    if options.get("id") is not None:
        params.append(f"id={quote(options['id'])}")
    if options.get("name") is not None:
        params.append(f"name={quote(options['name'])}")
    if options.get("path") is not None:
        params.append(f"path={quote(options['path'])}")
    if options.get("tag") is not None:
        params.append(f"tag={quote(options['tag'])}")
    params.append(f"kind={quote(value['kind'])}")
    string += "?"
    string += "&".join(params)
    return string


def from_data_string(data: str) -> Module:
    """Convert a string to a module."""
    from tangram.assert_ import assert_
    from tangram.object_ import Object_

    parts = data.split("?", 1)
    item_string = parts[0]
    assert_(item_string is not None)

    kind: Optional[Kind] = None
    if isinstance(item_string, str) and (
        item_string.startswith(".") or item_string.startswith("/")
    ):
        item: Item = item_string
    else:
        item = Object_.with_id(item_string)

    options: Options = {}
    if len(parts) > 1:
        params = parts[1]
        for param in params.split("&"):
            key_value = param.split("=", 1)
            if len(key_value) != 2:
                raise ValueError("missing value")
            key, value = key_value
            decoded_value = unquote(value)
            if key == "artifact":
                options["artifact"] = decoded_value
            elif key == "id":
                options["id"] = decoded_value
            elif key == "name":
                options["name"] = decoded_value
            elif key == "path":
                options["path"] = decoded_value
            elif key == "tag":
                options["tag"] = decoded_value
            elif key == "kind":
                kind = decoded_value  # type: ignore
            else:
                raise ValueError("invalid key")

    assert_(kind is not None)
    return {"kind": kind, "referent": Referent(item, options)}


def children(value: Module) -> List["Object_"]:
    """Get child objects from a module."""
    item = value["referent"].item
    if isinstance(item, str):
        return []
    # For graph edges, return the object if it exists.
    from tangram.object_ import Object_

    if Object_.is_(item):
        return [item]
    return []
