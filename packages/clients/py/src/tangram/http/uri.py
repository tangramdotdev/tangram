import math
from collections.abc import Mapping, Sequence
from typing import NotRequired, TypedDict, cast

type QueryPrimitive = bool | int | float | str | None
type QueryArray = Sequence["QueryValue"]
type QueryObject = Mapping[str, "QueryValue"]
type QueryValue = QueryArray | QueryObject | QueryPrimitive


class Arg(TypedDict):
    path: str
    query: NotRequired[str | Mapping[str, QueryValue] | None]


class Uri:
    Arg = Arg
    path: str
    query: str | None

    def __init__(
        self,
        path: str | Arg,
        query: Mapping[str, QueryValue] | str | None = None,
    ):
        if isinstance(path, str):
            index = path.find("?")
            if index == -1:
                self.path = path
                self.query = None
            else:
                self.path = path[:index]
                self.query = path[index + 1 :]
            if query is not None:
                self.query = query if isinstance(query, str) else query_string(query)
        else:
            self.path = path["path"]
            query = path.get("query")
            if isinstance(query, str):
                self.query = query
            else:
                self.query = query_string(query or {})

    def __str__(self) -> str:
        if self.query is None or self.query == "":
            return self.path
        return f"{self.path}?{self.query}"


def query_string(arg: Mapping[str, QueryValue]) -> str:
    params: list[str] = []
    for name, value in arg.items():
        append_query_param(params, name, value)
    return "&".join(params)


def append_query_param(params: list[str], name: str, value: QueryValue) -> None:
    if value is None:
        return
    if isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            append_query_param(params, f"{name}[{index}]", child)
    elif isinstance(value, Mapping):
        for key, child in value.items():
            append_query_param(params, f"{name}[{key}]", child)
    else:
        string = primitive_string(cast(QueryPrimitive, value))
        params.append(f"{percent_encode(name)}={percent_encode(string)}")


def percent_encode(value: str) -> str:
    # TextEncoder replaces unpaired surrogates and combines surrogate pairs.
    value = value.encode("utf-16-le", errors="surrogatepass").decode(
        "utf-16-le", errors="replace"
    )
    output = ""
    for byte in value.encode("utf-8"):
        if (
            0x41 <= byte <= 0x5A
            or 0x61 <= byte <= 0x7A
            or 0x30 <= byte <= 0x39
            or byte in (0x2D, 0x2E, 0x5F, 0x7E)
        ):
            output += chr(byte)
        else:
            output += f"%{byte:02X}"
    return output


def primitive_string(value: QueryPrimitive) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        if value == 0:
            return "0"
        text = repr(value)
        if "e" in text:
            mantissa, exponent = text.split("e")
            exponent = int(exponent)
            if -6 <= exponent < 21:
                from decimal import Decimal

                return format(Decimal(text), "f")
            mantissa = mantissa.removesuffix(".0")
            return f"{mantissa}e{'+' if exponent >= 0 else ''}{exponent}"
        return text.removesuffix(".0")
    return str(value)
