"""Independent byte and message flow control."""

from typing import TypedDict


class Limits(TypedDict):
    bytes: int
    messages: int


class Consumption(TypedDict):
    bytes: int
    messages: int


def validate_limits(limits: Limits) -> None:
    if (
        not _valid(limits["bytes"])
        or limits["bytes"] == 0
        or not _valid(limits["messages"])
        or limits["messages"] == 0
        or limits["messages"] > ((2**53 - 1) - 4) // 2
    ):
        raise ValueError("invalid flow limits")


class Sender:
    def __init__(self, limits: Limits):
        validate_limits(limits)
        self._consumption: Consumption = {"bytes": 0, "messages": 0}
        self._limits = limits.copy()
        self._sent: Consumption = {"bytes": 0, "messages": 0}

    def remaining_bytes(self) -> int:
        return self._limits["bytes"] - (
            self._sent["bytes"] - self._consumption["bytes"]
        )

    def available(self, bytes: int) -> bool:
        return (
            _valid(bytes)
            and bytes <= self.remaining_bytes()
            and self._sent["messages"] - self._consumption["messages"]
            < self._limits["messages"]
        )

    def send(self, bytes: int) -> None:
        if not self.available(bytes):
            raise ValueError("the flow window was exceeded")
        self._sent = _add(self._sent, bytes)

    def update(self, consumption: Consumption) -> None:
        if (
            not _valid(consumption["bytes"])
            or not _valid(consumption["messages"])
            or consumption["bytes"] < self._consumption["bytes"]
            or consumption["messages"] < self._consumption["messages"]
            or consumption["bytes"] > self._sent["bytes"]
            or consumption["messages"] > self._sent["messages"]
        ):
            raise ValueError("invalid flow consumption")
        self._consumption = consumption.copy()


class Receiver:
    def __init__(self, limits: Limits):
        validate_limits(limits)
        self._consumption: Consumption = {"bytes": 0, "messages": 0}
        self._limits = limits.copy()
        self._received: Consumption = {"bytes": 0, "messages": 0}
        self._reported: Consumption = {"bytes": 0, "messages": 0}

    def receive(self, bytes: int) -> None:
        received = _add(self._received, bytes)
        if (
            received["bytes"] - self._consumption["bytes"] > self._limits["bytes"]
            or received["messages"] - self._consumption["messages"]
            > self._limits["messages"]
        ):
            raise ValueError("the flow window was exceeded")
        self._received = received

    def consume(self, bytes: int) -> Consumption | None:
        consumption = _add(self._consumption, bytes)
        if (
            consumption["bytes"] > self._received["bytes"]
            or consumption["messages"] > self._received["messages"]
        ):
            raise ValueError("invalid flow consumption")
        self._consumption = consumption
        if (
            self._consumption["bytes"] - self._reported["bytes"]
            < (self._limits["bytes"] + 1) // 2
            and self._consumption["messages"] - self._reported["messages"]
            < (self._limits["messages"] + 1) // 2
        ):
            return None
        return self.flush()

    def flush(self) -> Consumption | None:
        if self._consumption == self._reported:
            return None
        self._reported = self._consumption.copy()
        return self._consumption.copy()


def _add(value: Consumption, bytes: int) -> Consumption:
    if not _valid(bytes) or not _valid(value["bytes"] + bytes):
        raise ValueError("the flow byte count overflowed")
    if not _valid(value["messages"] + 1):
        raise ValueError("the flow message count overflowed")
    return {"bytes": value["bytes"] + bytes, "messages": value["messages"] + 1}


def _valid(value: int) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and 0 <= value <= 2**53 - 1
    )
