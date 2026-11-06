"""Authorization proof normalization; the server verifies signatures and expiration."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Token:
    Data = str

    body: Body | None
    secret_id: int | None
    signature: bytes
    _data: str = field(repr=False)

    @classmethod
    def from_data(cls, data: str) -> Token:
        try:
            return decode(data)
        except ValueError:
            # Retain opaque tokens for the server to validate.
            return cls(None, None, b"", data)

    def to_data(self) -> str:
        return self._data

    def covers(self, other: Token) -> bool:
        a, b = self.body, other.body
        return self._data == other._data or (
            a is not None
            and b is not None
            and a.resource == b.resource
            and all(self.authorizes(b.resource, needed) for needed in b.permissions)
        )

    def authorizes(self, resource: str, permission: str) -> bool:
        return (
            self.body is not None
            and self.body.resource == resource
            and any(implies(granted, permission) for granted in self.body.permissions)
        )

    def authorizes_object_subtree(self, resource: str) -> bool:
        return self.authorizes(resource, "object_subtree")


@dataclass(frozen=True)
class Body:
    expires_at: int
    permissions: tuple[str, ...]
    resource: str


@dataclass
class Entry:
    authorization: list[Token] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not self.authorization

    def inherit(self, parent: Entry) -> None:
        self.authorization.extend(parent.authorization)
        self.normalize()

    def normalize(self, resource: str | None = None) -> None:
        # Keep the latest equivalent proofs per resource.
        resources: dict[str, list[Token]] = {}
        authorization = []
        distinct = {token.to_data(): token for token in self.authorization}
        for token in sorted(distinct.values(), key=compare):
            if token.body is None:
                authorization.append(token)
                continue
            token_resource = token.body.resource
            proofs = resources.get(token_resource, [])
            if any(existing.covers(token) for existing in proofs):
                continue
            proofs = [existing for existing in proofs if not token.covers(existing)]
            proofs.append(token)
            resources[token_resource] = proofs
        for proofs in resources.values():
            authorization.extend(proofs)
        # Keep sync proofs so readers can wait for objects still being transferred.
        if resource is not None and any(
            token.authorizes_object_subtree(resource) for token in authorization
        ):
            authorization = [
                token
                for token in authorization
                if (token.body is not None and token.body.resource.startswith("syn_"))
                or token.authorizes_object_subtree(resource)
            ]
        authorization.sort(key=lambda token: token.to_data())
        self.authorization = authorization


class TokensData(dict[str, list[str]]):
    @staticmethod
    def clone(
        tokens: Tokens | Mapping[str, Sequence[str]] | None,
    ) -> dict[str, list[str]]:
        if isinstance(tokens, Tokens):
            return tokens.to_data()
        return {location: list(entry) for location, entry in (tokens or {}).items()}

    @staticmethod
    def is_empty(tokens: Tokens | Mapping[str, Sequence[str]]) -> bool:
        if isinstance(tokens, Tokens):
            return tokens.is_empty()
        return all(not entry for entry in tokens.values())

    @staticmethod
    def local(tokens: dict[str, list[str]]) -> list[str] | None:
        return tokens.get("local")

    @staticmethod
    def with_local(entry: list[str] | None) -> dict[str, list[str]]:
        tokens = {} if not entry else {"local": list(entry)}
        TokensData.normalize(tokens)
        return tokens

    @staticmethod
    def inherit(
        tokens: dict[str, list[str]],
        parent: Tokens | Mapping[str, Sequence[str]],
        resource: str | None = None,
    ) -> dict[str, list[str]]:
        parsed = Tokens.from_data(tokens)
        parsed.inherit(Tokens.from_data(parent), resource)
        tokens.clear()
        tokens.update(parsed.to_data())
        return tokens

    @staticmethod
    def normalize(
        tokens: dict[str, list[str]], resource: str | None = None
    ) -> dict[str, list[str]]:
        parsed = Tokens.from_data(tokens)
        parsed.normalize(resource)
        tokens.clear()
        tokens.update(parsed.to_data())
        return tokens


class Tokens:
    Data = TokensData
    Entry = Entry

    def __init__(self):
        self._map: dict[str, Entry] = {}

    @classmethod
    def from_data(cls, data: Tokens | Mapping[str, Sequence[str]] | None) -> Tokens:
        if isinstance(data, Tokens):
            return data.clone()
        tokens = cls()
        for location, entry in (data or {}).items():
            tokens._map[location] = Entry([Token.from_data(token) for token in entry])
        return tokens

    def to_data(self) -> dict[str, list[str]]:
        return {
            location: [token.to_data() for token in entry.authorization]
            for location, entry in self._map.items()
        }

    def clone(self) -> Tokens:
        tokens = Tokens()
        for location, entry in self._map.items():
            tokens._map[location] = Entry(list(entry.authorization))
        return tokens

    def iter(self):
        return self._map.items()

    def get(self, location: str) -> Entry | None:
        return self._map.get(location)

    def is_empty(self) -> bool:
        return all(entry.is_empty() for entry in self._map.values())

    def set(self, location: str, entry: Entry) -> None:
        entry.normalize()
        self._map[location] = entry

    def inherit(self, parent: Tokens, resource: str | None = None) -> None:
        for location, entry in parent.iter():
            previous = self.get(location)
            authorization = [
                *(previous.authorization if previous is not None else []),
                *entry.authorization,
            ]
            self._map[location] = Entry(authorization)
        self.normalize(resource)

    def normalize(self, resource: str | None = None) -> None:
        for location, entry in list(self._map.items()):
            entry.normalize(resource)
            if entry.is_empty():
                del self._map[location]


def compare(token: Token) -> tuple[int, str]:
    return (-(token.body.expires_at if token.body is not None else -1), token.to_data())


def decode(token: str) -> Token:
    """Decode the body, secret ID, and signature."""
    data = decode_base32(token)
    position = 0
    if data[position : position + 1] != b"\x00":
        raise ValueError("unsupported token version")
    position += 1
    expiration, position = read_uvarint(data, position)
    if expiration >= 1 << 63:
        raise ValueError("invalid expiration")
    mask, position = read_uvarint(data, position)
    resource, kind, position = read_id(data, position)
    names = _permission_names.get(kind)
    if names is None:
        raise ValueError("invalid resource")
    permissions: list[str] = []
    for id in range(64):
        if (mask >> id) & 1:
            if id >= len(names):
                raise ValueError("invalid permissions")
            permissions.append(f"{kind}_{names[id]}")
    secret_id, position = read_uvarint(data, position)
    if position >= len(data):
        raise ValueError("missing the signature")
    body = Body(expiration, tuple(permissions), resource)
    return Token(body, secret_id, data[position:], token)


_symbols = "0123456789abcdefghjkmnpqrstvwxyz"


def decode_base32(value: str) -> bytes:
    output = bytearray()
    buffer = 0
    bits = 0
    for character in value:
        symbol = _symbols.find(character)
        if symbol == -1:
            raise ValueError("invalid symbol")
        buffer = ((buffer << 5) | symbol) & 0xFFF
        bits += 5
        if bits >= 8:
            bits -= 8
            output.append((buffer >> bits) & 0xFF)
    if bits >= 5 or (buffer & ((1 << bits) - 1)) != 0:
        raise ValueError("invalid trailing bits")
    return bytes(output)


def encode_base32(data: bytes) -> str:
    output = ""
    buffer = 0
    bits = 0
    for byte in data:
        buffer = ((buffer << 8) | byte) & 0xFFF
        bits += 8
        while bits >= 5:
            bits -= 5
            output += _symbols[(buffer >> bits) & 31]
    if bits > 0:
        output += _symbols[(buffer << (5 - bits)) & 31]
    return output


def read_uvarint(data: bytes, position: int) -> tuple[int, int]:
    value = 0
    for shift in range(0, 64, 7):
        if position >= len(data):
            raise ValueError("truncated varint")
        byte = data[position]
        position += 1
        value |= (byte & 0x7F) << shift
        if byte & 0x80 == 0:
            if value >= 1 << 64:
                raise ValueError("varint overflow")
            return value, position
    raise ValueError("varint overflow")


def read_id(data: bytes, position: int) -> tuple[str, str, int]:
    """Read a binary ID and return it with its resource kind."""
    header = data[position : position + 4]
    if len(header) != 4:
        raise ValueError("invalid ID")
    version, _, kind, algorithm = header
    prefix = _id_prefixes.get(kind)
    length = 16 if algorithm == 0 else 32 if algorithm == 1 else None
    if version != 0 or prefix is None or length is None:
        raise ValueError("invalid ID")
    body = data[position + 4 : position + 4 + length]
    if len(body) != length:
        raise ValueError("invalid ID")
    id = f"{prefix}_0{algorithm}{encode_base32(body)}"
    resource_kind = _resource_kinds.get(prefix, "")
    return id, resource_kind, position + 4 + length


_id_prefixes = {
    0: "blb",
    1: "dir",
    2: "fil",
    3: "sym",
    4: "gph",
    5: "cmd",
    6: "err",
    7: "sbx",
    8: "pcs",
    9: "usr",
    10: "grp",
    11: "org",
    12: "tag",
    13: "rnr",
    14: "sch",
    15: "tok",
    16: "idx",
    17: "syn",
    18: "rmt",
    19: "grt",
}

_resource_kinds = {
    "blb": "object",
    "cmd": "object",
    "dir": "object",
    "err": "object",
    "fil": "object",
    "gph": "object",
    "grp": "group",
    "org": "organization",
    "pcs": "process",
    "sbx": "sandbox",
    "sym": "object",
    "syn": "sync",
    "tag": "tag",
    "usr": "user",
}

# List each resource kind's permissions in the order of their IDs.
_permission_names = {
    "group": ["admin", "read", "write"],
    "object": ["node", "subtree"],
    "organization": ["admin", "read", "write"],
    "process": [
        "node",
        "node_command_objects",
        "node_error_objects",
        "node_log_objects",
        "node_output_objects",
        "parent",
        "subtree",
        "subtree_command_objects",
        "subtree_error_objects",
        "subtree_log_objects",
        "subtree_output_objects",
    ],
    "sandbox": ["node", "parent"],
    "sync": ["read"],
    "tag": ["admin", "read", "write"],
    "user": ["admin", "read", "write"],
}


def implies(granted: str, needed: str) -> bool:
    if granted == needed:
        return True
    if needed == "object_node":
        return granted == "object_subtree"
    process = (
        "process_node",
        "process_node_command_objects",
        "process_node_error_objects",
        "process_node_log_objects",
        "process_node_output_objects",
        "process_parent",
        "process_subtree",
        "process_subtree_command_objects",
        "process_subtree_error_objects",
        "process_subtree_log_objects",
        "process_subtree_output_objects",
    )
    if needed in process:
        return granted in (
            "process_parent",
            needed.replace("process_node", "process_subtree"),
        )
    if needed == "sandbox_node":
        return granted == "sandbox_parent"
    for kind in ("group", "organization", "tag", "user"):
        if needed == f"{kind}_read":
            return granted in (f"{kind}_write", f"{kind}_admin")
        if needed == f"{kind}_write":
            return granted == f"{kind}_admin"
    return False


class Authorization:
    Body = Body
    Token = Token
    Tokens = Tokens
