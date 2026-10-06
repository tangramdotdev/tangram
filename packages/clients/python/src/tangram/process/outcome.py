"""Process outcomes and wire values."""

from __future__ import annotations

from typing import TYPE_CHECKING, NotRequired, TypedDict

from ..error import Error
from ..object import Object
from ..referent import Referent
from ..value import Value

if TYPE_CHECKING:
    from ..error import ErrorData
    from ..location import LocationObject
    from ..value import ValueData, ValueType


class ProcessOutcome[O: ValueType](TypedDict):
    error: Error | None
    exit: int
    output: NotRequired[O]


class Outcome:
    class Data(TypedDict):
        error: NotRequired[ErrorData | str | None]
        exit: int
        output: NotRequired[ValueData]

    @staticmethod
    def from_data(data: Outcome.Data) -> ProcessOutcome[ValueType]:
        error = data.get("error")
        error = (
            (
                Error.with_referent(Referent.from_data_string(error))
                if isinstance(error, str)
                else Error.from_data(error)
            )
            if error is not None
            else None
        )
        output: ProcessOutcome[ValueType] = {"error": error, "exit": data["exit"]}
        if "output" in data:
            output["output"] = Value.from_data(data["output"])
        return output

    @staticmethod
    def inherit_location[O: ValueType](
        outcome: ProcessOutcome[O], location: LocationObject | None
    ) -> None:
        if outcome.get("error") is not None:
            Object.inherit_location(outcome["error"], location)
        if "output" in outcome:
            Value.inherit_location(outcome["output"], location)

    @staticmethod
    def inherit_tokens[O: ValueType](
        outcome: ProcessOutcome[O], tokens: dict[str, list[str]]
    ) -> None:
        if outcome.get("error") is not None:
            Object.inherit_tokens(outcome["error"], tokens)
        if "output" in outcome:
            Value.inherit_tokens(outcome["output"], tokens)

    @staticmethod
    def to_data[O: ValueType](value: ProcessOutcome[O]) -> Outcome.Data:
        output: Outcome.Data = {"exit": value["exit"]}
        error = value.get("error")
        if error is not None:
            output["error"] = Error.to_data_or_id(error)
        if "output" in value:
            output["output"] = Value.to_data(value["output"])
        return output
