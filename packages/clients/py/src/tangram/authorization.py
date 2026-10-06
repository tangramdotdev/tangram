"""Authorization proof normalization; the server verifies signatures and expiration."""

import base64
import json
import re
from collections.abc import Mapping, Sequence
from typing import TypedDict, cast


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


def parse(token: str) -> TokenBody | None:
    if token in _cache:
        return _cache[token]
    output = None
    try:
        parts = token.split(".")
        if len(parts) == 4 and parts[0] == "0":

            def decode(value: str) -> str:
                return base64.b64decode(value + "=" * (-len(value) % 4)).decode("utf-8")

            body_string = decode(parts[1])
            # Match JS integer syntax and reject duplicate expiration fields.
            expiration = re.findall(r'"expires_at"\s*:\s*(-?\d+)', body_string)
            if len(expiration) != 1:
                return None
            body = json.loads(
                re.sub(r'("expires_at"\s*:\s*)(-?\d+)', r'\1"\2"', body_string, count=1)
            )
            body["expires_at"] = int(body["expires_at"])
            metadata = json.loads(decode(parts[2]))
            if (
                isinstance(body["resource"], str)
                and -(2**63) <= body["expires_at"] < 2**63
                and isinstance(body["permissions"], list)
                and all(
                    isinstance(permission, str) for permission in body["permissions"]
                )
                and isinstance(metadata["algorithm"], str)
                and isinstance(metadata["key"], str)
            ):
                output = cast(TokenBody, body)
    except (ValueError, KeyError, TypeError, UnicodeError):
        # Opaque tokens are retained for the server to validate.
        pass
    if len(_cache) >= 1024:
        _cache.clear()
    _cache[token] = output
    return output


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


def normalize(
    tokens: dict[str, list[str]], resource: str | None = None
) -> dict[str, list[str]]:
    # Compare proofs independently at each location and within the same resource.
    for location, entry in list(tokens.items()):
        resources: dict[str, list[str]] = {}
        authorization = []
        for token in sorted(set(entry)):
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
