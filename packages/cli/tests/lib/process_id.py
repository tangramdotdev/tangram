import base64
import os
import time

import blake3

ALPHABET = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ234567", "0123456789abcdefghjkmnpqrstvwxyz")
DECODE = str.maketrans("0123456789abcdefghjkmnpqrstvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567")


def encode(data):
    return base64.b32encode(data).decode().rstrip("=").translate(ALPHABET)


def decode(text):
    text = text.translate(DECODE)
    return base64.b32decode(text + "=" * (-len(text) % 8))


def uuidv7(timestamp, random):
    body = bytearray(timestamp.to_bytes(6, "big") + random[:10])
    body[6] = (body[6] & 0x0F) | 0x70
    body[8] = (body[8] & 0x3F) | 0x80
    return bytes(body)


def process_id():
    # Create an ID with a random UUIDv7 body.
    body = uuidv7(int(time.time() * 1000), os.urandom(10))
    return "pcs_00" + encode(body)


def derived_process_id(runner):
    # Derive an ID from a runner's ID and a random salt, and return it with its proof.
    runner_bytes = bytes([0, 0, 13, 0]) + decode(runner[6:])
    salt = os.urandom(16)
    hash = blake3.derive_key("tangram process id v1", runner_bytes + salt)
    body = uuidv7(int(time.time() * 1000), hash)
    proof = {"kind": "v1", "value": {"salt": encode(salt)}}
    return "pcs_00" + encode(body), proof
