"""Location type and utilities.

This module mirrors the JavaScript packages/clients/js/src/location.ts.
"""

from __future__ import annotations

from typing import TypedDict

from tangram.module_ import Module, ModuleData
from tangram.module_ import from_data as module_from_data
from tangram.module_ import to_data as module_to_data
from tangram.range_ import Range


class Location(TypedDict):
    """A location in a module with a range."""

    module: Module
    range: Range


class LocationData(TypedDict):
    """Serialized location data."""

    module: ModuleData
    range: Range


def to_data(value: Location) -> LocationData:
    """Convert a location to data."""
    return {
        "module": module_to_data(value["module"]),
        "range": value["range"],
    }


def from_data(data: LocationData) -> Location:
    """Convert data to a location."""
    return {
        "module": module_from_data(data["module"]),
        "range": data["range"],
    }
