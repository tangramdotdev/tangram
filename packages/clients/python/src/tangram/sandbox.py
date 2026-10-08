"""Sandbox handles, arguments, and fluent construction."""

from __future__ import annotations

from collections.abc import Generator
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    NotRequired,
    Required,
    Self,
    TypedDict,
    cast,
)

if TYPE_CHECKING:
    from .client import Client
    from .client.sandbox.destroy import ArgObject as SandboxDestroyArg
    from .client.sandbox.get import ArgObject as SandboxGetArg
    from .process import Builder as ProcessBuilder

from . import authorization, host
from .location import Arg as LocationArg
from .location import ArgObject, LocationObject
from .mutation import Mutation
from .resolve import Unresolved, capture

type IsolationValue = Literal["container", "seatbelt", "vm"]
type NetworkKind = Literal["default", "host", "bridge"]


class MountValue(TypedDict):
    source: str
    target: str
    readonly: NotRequired[bool]


class NetworkBridge(TypedDict, total=False):
    kind: Literal["bridge"]
    ports: list[str]


type NetworkValue = NetworkKind | NetworkBridge


class SandboxNetworkData(TypedDict):
    kind: NetworkKind
    ports: NotRequired[list[str]]


class Cpu(TypedDict, total=False):
    dedicated: int
    shared: int


class SandboxArg(TypedDict, total=False):
    cpu: Unresolved[int | Cpu | Mutation | None]
    host: Unresolved[str | Mutation | None]
    hostname: Unresolved[str | Mutation | None]
    isolation: Unresolved[IsolationValue | Mutation | None]
    location: Unresolved[str | ArgObject | None]
    memory: Unresolved[int | float | Mutation | None]
    mounts: Unresolved[list[Unresolved[MountValue]] | None]
    network: Unresolved[bool | NetworkValue | Mutation | None]
    owner: Unresolved[str | Mutation | None]
    ports: Unresolved[list[Unresolved[str]] | None]
    ttl: Unresolved[int | float | Mutation | None]


type SandboxInput = Unresolved[
    SandboxArg | list[SandboxInput] | tuple[SandboxInput, ...]
]


class SandboxUsage(TypedDict):
    cpu: Cpu
    memory: int | float


class SandboxData(TypedDict, total=False):
    id: Required[str]
    status: Required[Literal["created", "started", "destroyed"]]
    cpu: Cpu | None
    creator: str | None
    hostname: str | None
    isolation: dict[str, IsolationValue] | None
    memory: int | float | None
    mounts: list[str]
    network: SandboxNetworkData | None
    owner: str | None
    ttl: int | float | None
    usage: SandboxUsage | None


class SandboxOutput(TypedDict):
    data: SandboxData
    location: NotRequired[LocationObject | None]
    tokens: NotRequired[dict[str, list[str]] | None]


class SandboxConstructorArg(TypedDict, total=False):
    id: Required[str]
    location: str | ArgObject | None
    owned: bool
    state: SandboxOutput | None
    tokens: dict[str, list[str]] | None


class Sandbox:
    Arg: ClassVar[type[Arg]]
    Builder: ClassVar[type[Builder]]
    Create: ClassVar[type[Create]]
    Cpu = Cpu
    Data = SandboxData
    DataArg = dict
    Destroy: ClassVar[type[Destroy]]
    Get: ClassVar[type[Get]]
    Id: ClassVar[type[Id]]
    Isolation: ClassVar[type[Isolation]]
    Mount: ClassVar[type[Mount]]
    Network: ClassVar[type[Network]]
    Port: ClassVar[type[Port]]
    ConstructorArg = SandboxConstructorArg
    Source = dict
    Status = Literal["created", "started", "destroyed"]
    Usage = SandboxUsage
    __tangram_atomic__ = True

    def __init__(
        self,
        id,
        *,
        client: Client | None = None,
        location=None,
        tokens=None,
        data=None,
        state=None,
        owned=False,
    ):
        from .client import client as default_client

        if isinstance(id, dict):
            arg = id
            id = arg["id"]
            location = arg.get("location", location)
            owned = arg.get("owned", owned)
            state = arg.get("state", state)
            tokens = arg.get("tokens", tokens)
        self.client = client or default_client
        self.id = id
        self.location = _location_arg(location)
        self.owned = owned
        self.state = (
            state
            if state is not None
            else ({"data": data} if data is not None else None)
        )
        self._tokens = authorization.clone(tokens)
        authorization.normalize(self._tokens)
        if self.state is not None:
            authorization.inherit(self._tokens, self.state.get("tokens") or {})

    @property
    def data(self) -> SandboxData | None:
        return self.state["data"] if self.state is not None else None

    @property
    def tokens(self) -> dict[str, list[str]]:
        return authorization.clone(self._tokens)

    @classmethod
    def create(
        cls, *args: SandboxInput, client: Client | None = None, **options
    ) -> Builder:
        return Builder(*args, *([options] if options else []), client=client)

    @classmethod
    def with_id(cls, id: str, location=None, *, client: Client | None = None) -> Self:
        return cls(id, location=location, client=client)

    @staticmethod
    def expect(value: object) -> Sandbox:
        assert isinstance(value, Sandbox)
        return value

    @staticmethod
    def assert_(value: object) -> None:
        assert isinstance(value, Sandbox)

    async def load(self, client: Client | None = None) -> None:
        client = client or self.client
        arg: SandboxGetArg = {"tokens": self._tokens}
        if self.location is not None:
            arg["location"] = self.location
        output = await client.get_sandbox(self.id, **arg)
        if output.get("tokens") is not None and not authorization.is_empty(
            output["tokens"] or {}
        ):
            tokens = authorization.clone(output["tokens"])
            authorization.inherit(tokens, self._tokens)
            self._tokens = tokens
        self.location = _location_arg(output.get("location"))
        self.state = output

    async def reload(self, client: Client | None = None) -> None:
        await self.load(client)

    async def destroy(self, client: Client | None = None) -> None:
        client = client or self.client
        arg: SandboxDestroyArg = {}
        if self.location is not None:
            arg["location"] = self.location
        await client.destroy_sandbox(self.id, **arg)
        self.detach()

    def detach(self) -> None:
        self.owned = False

    def run(self, *args) -> ProcessBuilder:
        from .process.run import builder as run

        builder = run(*args, client=self.client).sandbox(self.id)
        if self.location is not None:
            builder.location(self.location)
        return builder

    async def close(self) -> None:
        if self.owned:
            arg: SandboxDestroyArg = {}
            if self.location is not None:
                arg["location"] = self.location
            await self.client.try_destroy_sandbox(self.id, **arg)
            self.detach()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()


class Builder:
    def __init__(self, *args: SandboxInput, client: Client | None = None) -> None:
        from .client import client as default_client

        self._memo = {}
        self._args = capture(args, self._memo)
        self.client = client or default_client

    def _push(self, name, value) -> Self:
        self._args.append(capture({name: value}, self._memo))
        return self

    def cpu(self, value: Unresolved[int | Cpu | Mutation | None]) -> Self:
        return self._push("cpu", value)

    def host(self, value: Unresolved[str | Mutation | None]) -> Self:
        return self._push("host", value)

    def hostname(self, value: Unresolved[str | Mutation | None]) -> Self:
        return self._push("hostname", value)

    def isolation(self, value: Unresolved[IsolationValue | Mutation | None]) -> Self:
        return self._push("isolation", value)

    def location(self, value: Unresolved[str | ArgObject | Mutation | None]) -> Self:
        return self._push("location", value)

    def memory(self, value: Unresolved[int | float | Mutation | None]) -> Self:
        return self._push("memory", value)

    def mount(self, *values: Unresolved[MountValue]) -> Self:
        return self._push("mounts", values)

    def mounts(self, *values: Unresolved[list[MountValue] | Mutation | None]) -> Self:
        for value in values:
            self._push("mounts", value)
        return self

    def network(
        self, value: Unresolved[bool | NetworkValue | Mutation | None] = True
    ) -> Self:
        return self._push("network", value)

    def owner(self, value: Unresolved[str | Mutation | None]) -> Self:
        return self._push("owner", value)

    def port(self, *values: Unresolved[str]) -> Self:
        return self._push("ports", values)

    def ports(self, *values: Unresolved[list[str] | Mutation | None]) -> Self:
        for value in values:
            self._push("ports", value)
        return self

    def ttl(self, value: Unresolved[int | float | Mutation | None]) -> Self:
        return self._push("ttl", value)

    def __await__(self) -> Generator[Any, None, Sandbox]:
        return self._create().__await__()

    async def _create(self) -> Sandbox:
        from .args import Args

        arg = await Args.apply(
            args=[{"host": host.current, "ttl": 300}, *self._args],
            map=lambda arg: arg,
            reduce={"mounts": "append", "ports": "append"},
        )
        output = await self.client.create_sandbox(arg_to_data(arg))
        return Sandbox(
            output["data"]["id"],
            client=self.client,
            location=_location_arg(output.get("location")),
            state=output,
            owned=True,
        )


class Mount:
    Data = str

    @staticmethod
    def to_data_string(value: MountValue) -> str:
        return (
            value["source"]
            + ":"
            + value["target"]
            + (",ro" if value.get("readonly") is True else "")
        )

    @staticmethod
    def from_data_string(data: str) -> MountValue:
        string, comma, option = data.partition(",")
        if comma and option not in ("ro", "rw"):
            raise ValueError(f"unknown option: {option}")
        source, colon, target = string.partition(":")
        if not colon:
            raise ValueError("expected a target path")
        if not target.startswith("/"):
            raise ValueError("expected an absolute path")
        return {"source": source, "target": target, "readonly": option == "ro"}


def normalize_network(value, ports=()):
    network = None
    if value is True:
        network = {"kind": "default"}
    elif value is not None and value is not False:
        network = Network.to_data(value)
    if not ports:
        return network
    if value is False:
        raise ValueError("ports require networking")
    if network is not None and network["kind"] == "host":
        raise ValueError("ports are not supported with host networking")
    return {"kind": "bridge", "ports": [*((network or {}).get("ports") or []), *ports]}


def arg_to_data(arg):
    output = {
        key: arg[key]
        for key in ("cpu", "host", "hostname", "memory", "owner", "ttl")
        if key in arg
    }
    if "isolation" in arg:
        value = arg["isolation"]
        output["isolation"] = None if value is None else Isolation.to_data(value)
    if "location" in arg:
        output["location"] = _location_data(arg["location"])
    if arg.get("mounts") is not None:
        output["mounts"] = [Mount.to_data_string(value) for value in arg["mounts"]]
    network = normalize_network(arg.get("network"), arg.get("ports") or [])
    if network is not None:
        output["network"] = network
    if "cpu" in output and isinstance(output["cpu"], (int, float)):
        output["cpu"] = {"shared": output["cpu"]}
    return output


def _location_arg(value: str | ArgObject | LocationObject | None) -> ArgObject | None:
    if value is None:
        return None
    if isinstance(value, str):
        return LocationArg.from_data_string(value)
    return (
        cast(ArgObject, value)
        if "components" in value
        else LocationArg.from_location(value)
    )


def _location_data(value):
    return None if value is None else LocationArg.to_data_string(value)


Sandbox.Builder = Builder
Sandbox.Mount = Mount


class Port:
    Data = str

    @staticmethod
    def to_data_string(value) -> str:
        return value

    @staticmethod
    def from_data_string(data: str) -> str:
        return data


Sandbox.Port = Port


class Id:
    @staticmethod
    def is_(value):
        return isinstance(value, str) and value.startswith("sbx_")


class Arg:
    to_data = staticmethod(arg_to_data)


class Get:
    Arg = dict
    Output = SandboxOutput


class Create:
    Arg = dict
    Output = SandboxOutput


class Destroy:
    Arg = dict


class Isolation:
    Data = dict

    @staticmethod
    def to_data(value):
        return {"kind": value}

    @staticmethod
    def from_data(data):
        return data["kind"]


class Network:
    Bridge = dict
    Data = dict

    @staticmethod
    def to_data(value):
        if isinstance(value, str):
            return {"kind": value}
        if "ports" not in value:
            return {"kind": "bridge"}
        return {
            "kind": "bridge",
            "ports": [Port.to_data_string(port) for port in value["ports"]],
        }

    @staticmethod
    def from_data(data):
        if data["kind"] != "bridge":
            return data["kind"]
        if not data.get("ports"):
            return "bridge"
        return {
            "kind": "bridge",
            "ports": [Port.from_data_string(port) for port in data["ports"]],
        }


Sandbox.Arg = Arg
Sandbox.Create = Create
Sandbox.Destroy = Destroy
Sandbox.Get = Get
Sandbox.Id = Id
Sandbox.Isolation = Isolation
Sandbox.Network = Network
