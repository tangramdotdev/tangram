"""Diagnostic type and utilities.

This module mirrors the JavaScript packages/clients/js/src/diagnostic.ts.
"""

from __future__ import annotations

from typing import Literal, Optional, TypedDict

from tangram.location import Location, LocationData
from tangram.location import from_data as location_from_data
from tangram.location import to_data as location_to_data

Severity = Literal["error", "warning", "info", "hint"]


class Diagnostic(TypedDict, total=False):
    """A diagnostic message with optional location."""

    location: Optional[Location]
    message: str
    severity: Severity


class DiagnosticData(TypedDict, total=False):
    """Serialized diagnostic data."""

    location: Optional[LocationData]
    message: str
    severity: Severity


def to_data(value: Diagnostic) -> DiagnosticData:
    """Convert a diagnostic to data."""
    data: DiagnosticData = {
        "message": value["message"],
        "severity": value["severity"],
    }
    if value.get("location") is not None:
        data["location"] = location_to_data(value["location"])
    return data


def from_data(data: DiagnosticData) -> Diagnostic:
    """Convert data to a diagnostic."""
    diagnostic: Diagnostic = {
        "message": data["message"],
        "severity": data["severity"],
    }
    if data.get("location") is not None:
        diagnostic["location"] = location_from_data(data["location"])
    return diagnostic
