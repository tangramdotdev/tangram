from collections.abc import AsyncIterable, Mapping
from typing import NotRequired, TypedDict

from .body import Body, json_bytes
from .headers import Headers, HeaderValue
from .uri import Arg as UriArg
from .uri import QueryValue, Uri


class Arg(TypedDict):
    body: NotRequired[Body | AsyncIterable[str | bytes]]
    headers: NotRequired[Headers | Mapping[str, HeaderValue]]
    method: str
    uri: Uri | str | UriArg


class Request:
    Arg = Arg

    def __init__(
        self,
        method: str | Arg,
        uri: Uri | str | UriArg | None = None,
        headers: Headers | Mapping[str, HeaderValue] | None = None,
        body: Body | AsyncIterable[str | bytes] | bytes | str | None = None,
    ):
        if isinstance(method, Mapping):
            arg = method
            body = arg.get("body")
            headers = arg.get("headers")
            uri = arg["uri"]
            method = arg["method"]
        if uri is None:
            raise TypeError("a request URI is required")
        self.body = body if isinstance(body, Body) or body is None else Body(body)
        self.headers = headers if isinstance(headers, Headers) else Headers(headers)
        self.method = method
        self.uri = uri if isinstance(uri, Uri) else Uri(uri)

    def arg(self, arg: Mapping[str, QueryValue], body: Body | None = None) -> "Request":
        body = body if body is not None else self.body or Body.empty()
        uri = Uri({"path": self.uri.path, "query": arg})
        headers = self.headers.to_data()
        if len(uri.query or "") > 4096:
            data = json_bytes(arg)
            length = len(data)
            prefix = bytearray()
            while length >= 128:
                prefix.append((length % 128) | 128)
                length //= 128
            prefix.append(length)
            frame = bytes(prefix) + data
            body = body.prepend(frame)
            uri.query = None
            headers["x-tg-arg-in-body"] = "true"
            headers["cache-control"] = "no-store"
            headers.pop("content-length", None)
        else:
            headers.pop("x-tg-arg-in-body", None)
        self.body = body
        self.headers = Headers(headers)
        self.uri = uri
        return self
