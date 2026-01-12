"""Referent type and utilities.

This module mirrors the JavaScript packages/clients/js/src/referent.ts.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Generic, Optional, TypedDict, TypeVar, Union
from urllib.parse import quote, unquote

from tangram.tag import Tag

T = TypeVar("T")
U = TypeVar("U")


class Options(TypedDict, total=False):
    """Options for a referent."""

    artifact: Optional[str]
    id: Optional[str]
    name: Optional[str]
    path: Optional[str]
    tag: Optional[Tag]


class Referent(Generic[T]):
    """A referent wraps an item with options."""

    def __init__(self, item: T, options: Optional[Options] = None) -> None:
        """Initialize a referent."""
        self._item = item
        self._options = options if options is not None else {}

    @property
    def item(self) -> T:
        """Get the item."""
        return self._item

    @property
    def options(self) -> Options:
        """Get the options."""
        return self._options


class DataOptions(TypedDict, total=False):
    """Serialized options for a referent."""

    artifact: str
    id: str
    name: str
    path: str
    tag: Tag


class Data(Generic[T], TypedDict, total=False):
    """Serialized referent data."""

    item: T
    options: DataOptions


def to_data(value: Referent[T], f: Callable[[T], U]) -> Data[U]:
    """Convert a referent to data."""
    item = f(value.item)
    options: DataOptions = {}
    if value.options.get("artifact") is not None:
        options["artifact"] = value.options["artifact"]
    if value.options.get("id") is not None:
        options["id"] = value.options["id"]
    if value.options.get("name") is not None:
        options["name"] = value.options["name"]
    if value.options.get("path") is not None:
        options["path"] = value.options["path"]
    if value.options.get("tag") is not None:
        options["tag"] = value.options["tag"]
    return {"item": item, "options": options}


def from_data(data: Union[str, Data[T]], f: Callable[[T], U]) -> Referent[U]:
    """Convert data to a referent."""
    if isinstance(data, str):
        return from_data_string(data, f)
    item = f(data["item"])
    options: Options = data.get("options", {})
    return Referent(item, options)


def to_data_string(value: Referent[T], f: Callable[[T], str]) -> str:
    """Convert a referent to a string."""
    item = f(value.item)
    string = item
    params = []
    if value.options.get("artifact") is not None:
        params.append(f"artifact={quote(value.options['artifact'])}")
    if value.options.get("id") is not None:
        params.append(f"id={quote(value.options['id'])}")
    if value.options.get("name") is not None:
        params.append(f"name={quote(value.options['name'])}")
    if value.options.get("path") is not None:
        params.append(f"path={quote(value.options['path'])}")
    if value.options.get("tag") is not None:
        params.append(f"tag={quote(value.options['tag'])}")
    if params:
        string += "?"
        string += "&".join(params)
    return string


def from_data_string(data: str, f: Callable[[str], U]) -> Referent[U]:
    """Convert a string to a referent."""
    parts = data.split("?", 1)
    item_string = parts[0]
    params = parts[1] if len(parts) > 1 else None

    item = f(item_string)
    options: Options = {}

    if params is not None:
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
            else:
                raise ValueError("invalid key")

    return Referent(item, options)
