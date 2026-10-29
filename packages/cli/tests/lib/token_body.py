"""Print the body of an authorization token as JSON, without verifying the signature."""

import json
import sys

SYMBOLS = "0123456789abcdefghjkmnpqrstvwxyz"
PREFIXES = {
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
}
OBJECTS = {"blb", "cmd", "dir", "err", "fil", "gph", "sym"}
KINDS = {"grp": "group", "org": "organization", "pcs": "process", "sbx": "sandbox"}
KINDS |= {"syn": "sync", "tag": "tag", "usr": "user"}
PERMISSIONS = {
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


def decode(value):
    output, buffer, bits = bytearray(), 0, 0
    for character in value:
        buffer = ((buffer << 5) | SYMBOLS.index(character)) & 0xFFF
        bits += 5
        if bits >= 8:
            bits -= 8
            output.append((buffer >> bits) & 0xFF)
    return bytes(output)


def encode(data):
    output, buffer, bits = "", 0, 0
    for byte in data:
        buffer = ((buffer << 8) | byte) & 0xFFF
        bits += 8
        while bits >= 5:
            bits -= 5
            output += SYMBOLS[(buffer >> bits) & 31]
    if bits:
        output += SYMBOLS[(buffer << (5 - bits)) & 31]
    return output


def uvarint(data, position):
    value, shift = 0, 0
    while True:
        byte = data[position]
        position += 1
        value |= (byte & 0x7F) << shift
        shift += 7
        if byte & 0x80 == 0:
            return value, position


def body(token):
    data = decode(token.strip())
    expires_at, position = uvarint(data, 1)
    mask, position = uvarint(data, position)
    prefix, algorithm = PREFIXES[data[position + 2]], data[position + 3]
    length = 16 if algorithm == 0 else 32
    resource = f"{prefix}_0{algorithm}{encode(data[position + 4 : position + 4 + length])}"
    kind = "object" if prefix in OBJECTS else KINDS[prefix]
    names = PERMISSIONS[kind]
    permissions = [f"{kind}_{names[id]}" for id in range(64) if (mask >> id) & 1]
    return {"expires_at": expires_at, "permissions": permissions, "resource": resource}


if __name__ == "__main__":
    print(json.dumps(body(sys.argv[1])))
