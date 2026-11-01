"""Authorization proof normalization; the server verifies signatures and expiration."""

from collections.abc import Mapping, Sequence
from typing import TypedDict


class TokenBody(TypedDict):
    expires_at: int
    permissions: list[str]
    resource: str


_cache: dict[str, TokenBody | None] = {}


def covers(token: str, other: str) -> bool:
    """Compare resources and permissions without verifying signatures or expiration."""
    if token == other:
        return True
    a, b = parse(token), parse(other)
    return (
        a is not None
        and b is not None
        and a["resource"] == b["resource"]
        and all(authorizes(token, b["resource"], needed) for needed in b["permissions"])
    )


def authorizes(token: str, resource: str, permission: str) -> bool:
    data = parse(token)
    return (
        data is not None
        and data["resource"] == resource
        and any(implies(granted, permission) for granted in data["permissions"])
    )


def authorizes_object_subtree(token: str, resource: str) -> bool:
    return authorizes(token, resource, "object_subtree")


def resource(token: str) -> str | None:
    data = parse(token)
    return data["resource"] if data is not None else None


def expires_at(token: str) -> int | None:
    data = parse(token)
    return data["expires_at"] if data is not None else None


def parse(token: str) -> TokenBody | None:
    if token in _cache:
        return _cache[token]
    output = None
    try:
        output = decode(token)
    except ValueError:
        # Opaque tokens are retained for the server to validate.
        pass
    if len(_cache) >= 1024:
        _cache.clear()
    _cache[token] = output
    return output


def decode(token: str) -> TokenBody:
    """Decode the body that precedes the secret ID and the signature."""
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
    _, position = read_uvarint(data, position)
    if position >= len(data):
        raise ValueError("missing the signature")
    return {"expires_at": expiration, "permissions": permissions, "resource": resource}


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


def clone(tokens: Mapping[str, Sequence[str]] | None) -> dict[str, list[str]]:
    return {location: list(entry) for location, entry in (tokens or {}).items()}


def is_empty(tokens: dict[str, list[str]]) -> bool:
    return all(not entry for entry in tokens.values())


def local(tokens: dict[str, list[str]]) -> list[str] | None:
    return tokens.get("local")


def with_local(entry: list[str] | None) -> dict[str, list[str]]:
    tokens = {} if not entry else {"local": list(entry)}
    normalize(tokens)
    return tokens


def inherit(
    tokens: dict[str, list[str]],
    parent: Mapping[str, Sequence[str]],
    resource: str | None = None,
) -> dict[str, list[str]]:
    for location, entry in parent.items():
        tokens[location] = [*tokens.get(location, []), *entry]
    normalize(tokens, resource)
    return tokens


def compare(token: str) -> tuple[int, str]:
    expiration = expires_at(token)
    return (-(expiration if expiration is not None else -1), token)


def normalize(
    tokens: dict[str, list[str]], resource: str | None = None
) -> dict[str, list[str]]:
    # Compare proofs per location and resource, keeping the latest of equivalent proofs.
    for location, entry in list(tokens.items()):
        resources: dict[str, list[str]] = {}
        authorization = []
        for token in sorted(set(entry), key=compare):
            token_resource = Token.resource(token)
            if token_resource is None:
                authorization.append(token)
                continue
            proofs = resources.get(token_resource, [])
            if any(covers(existing, token) for existing in proofs):
                continue
            proofs = [existing for existing in proofs if not covers(token, existing)]
            proofs.append(token)
            resources[token_resource] = proofs
        for proofs in resources.values():
            authorization.extend(proofs)
        # Keep sync proofs so readers can wait for objects still being transferred.
        if resource is not None and any(
            authorizes_object_subtree(token, resource) for token in authorization
        ):
            authorization = [
                token
                for token in authorization
                if (Token.resource(token) or "").startswith("syn_")
                or authorizes_object_subtree(token, resource)
            ]
        authorization.sort()
        if not authorization:
            del tokens[location]
            continue
        tokens[location] = authorization
    return tokens


def _inherit(
    tokens: dict[str, list[str]],
    parent: Mapping[str, Sequence[str]],
    resource: str | None = None,
) -> None:
    inherit(tokens, parent, resource)


def _normalize(tokens: dict[str, list[str]], resource: str | None = None) -> None:
    normalize(tokens, resource)


# Preserve the public namespaces while retaining module functions for internal callers.
class Token(str):
    covers = staticmethod(covers)
    authorizes = staticmethod(authorizes)
    authorizes_object_subtree = staticmethod(authorizes_object_subtree)
    resource = staticmethod(resource)
    expires_at = staticmethod(expires_at)


class Tokens(dict[str, list[str]]):
    clone = staticmethod(clone)
    is_empty = staticmethod(is_empty)
    local = staticmethod(local)
    with_local = staticmethod(with_local)
    inherit = staticmethod(_inherit)
    normalize = staticmethod(_normalize)


class Authorization:
    Token = Token
    Tokens = Tokens


# Existing internal callers can use the shorter spelling.
authorizes_subtree = authorizes_object_subtree
