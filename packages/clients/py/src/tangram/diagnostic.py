"""Diagnostics and their wire representations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, NotRequired, TypedDict

if TYPE_CHECKING:
    from .module import ModuleLocationData, ModuleLocationObject
    from .object import Object

from .module import Module

type Severity = Literal["error", "warning", "info", "hint"]


class DiagnosticObject(TypedDict):
    location: ModuleLocationObject | None
    message: str
    severity: Severity


class DiagnosticData(TypedDict):
    location: NotRequired[ModuleLocationData | None]
    message: str
    severity: Severity


class Diagnostic(dict):
    Severity = Literal["error", "warning", "info", "hint"]

    @staticmethod
    def to_data(value: DiagnosticObject) -> DiagnosticData:
        output: DiagnosticData = {
            "message": value["message"],
            "severity": value["severity"],
        }
        location = value.get("location")
        if location is not None:
            output["location"] = Module.Location.to_data(location)
        return output

    @staticmethod
    def from_data(data: DiagnosticData) -> DiagnosticObject:
        location = data.get("location")
        return {
            "location": Module.Location.from_data(location)
            if location is not None
            else None,
            "message": data["message"],
            "severity": data["severity"],
        }

    @staticmethod
    def children(value: DiagnosticObject) -> list[Object]:
        if value.get("location") is not None:
            return Module.Location.children(value["location"])
        return []

    class Data(dict):
        @staticmethod
        def children(data: DiagnosticData) -> list[str]:
            if data.get("location") is not None:
                return Module.Location.Data.children(data["location"])
            return []

        @staticmethod
        def without_location_and_tokens(data: DiagnosticData) -> DiagnosticData:
            output: DiagnosticData = {**data}
            if data.get("location") is not None:
                output["location"] = Module.Location.Data.without_location_and_tokens(
                    data["location"]
                )
            return output
