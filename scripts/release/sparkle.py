"""Manage the Sparkle signing key and prepare a signed macOS update feed."""

import base64
import getpass
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

from sign import KEY_BUCKET, download_key, passphrase, signing_directory

KEY_OBJECT = "sparkle-ed25519.key.gpg"
PUBLIC_KEY = Path("packages/macos/SparklePublicKey.txt")
APPCAST = "appcast.xml"
MACOS_PREFIX = "stable/macos"
PACKAGE_HOST = "https://pkgs.tangram.dev"
BUILD_NUMBER = r"[1-9][0-9]*"


def key_pair(seed=None):
    program = """
import CryptoKit
import Foundation
let key: Curve25519.Signing.PrivateKey
if let seed = readLine(), let bytes = Data(base64Encoded: seed) {
    key = try Curve25519.Signing.PrivateKey(rawRepresentation: bytes)
} else {
    key = Curve25519.Signing.PrivateKey()
}
print(key.rawRepresentation.base64EncodedString())
print(key.publicKey.rawRepresentation.base64EncodedString())
"""
    output = subprocess.check_output(["swift", "-e", program], input=seed or b"")
    private, public = output.splitlines()
    if seed is not None and private != seed.strip():
        raise ValueError("the Sparkle signing key is invalid")
    return private, public.decode()


def gpg(directory, password, *args):
    return subprocess.check_output(
        [
            "gpg",
            "--homedir",
            str(directory),
            "--batch",
            "--no-tty",
            "--no-symkey-cache",
            "--pinentry-mode",
            "loopback",
            "--passphrase-fd",
            "0",
            *args,
        ],
        input=password,
    )


def bootstrap():
    if PUBLIC_KEY.exists():
        raise ValueError(f"{PUBLIC_KEY} already exists")
    with signing_directory() as directory:
        contents = json.loads(
            subprocess.check_output(
                [
                    "cf",
                    "r2",
                    "objects",
                    "list",
                    "--bucket-name",
                    KEY_BUCKET,
                    "--prefix",
                    KEY_OBJECT,
                ]
            )
        )
        password = (
            getpass.getpass("Tangram Sparkle signing passphrase: ").encode() + b"\n"
        )
        if password == b"\n":
            raise ValueError("expected a Sparkle signing passphrase")
        if any(entry["key"] == KEY_OBJECT for entry in contents):
            encrypted = download_key(directory, KEY_OBJECT)
            seed = gpg(directory, password, "--decrypt", str(encrypted))
            _, public = key_pair(seed)
        else:
            if (
                password
                != getpass.getpass("Confirm Sparkle signing passphrase: ").encode()
                + b"\n"
            ):
                raise ValueError("the Sparkle passphrases do not match")
            seed, public = key_pair()
            private = directory / "sparkle.key"
            private.touch(mode=0o600, exist_ok=False)
            private.write_bytes(seed + b"\n")
            encrypted = directory / KEY_OBJECT
            gpg(
                directory,
                password,
                "--cipher-algo",
                "AES256",
                "--output",
                str(encrypted),
                "--symmetric",
                str(private),
            )
            if gpg(directory, password, "--decrypt", str(encrypted)) != seed + b"\n":
                raise ValueError("could not verify the encrypted Sparkle key")
            subprocess.run(
                [
                    "cf",
                    "r2",
                    "objects",
                    "put",
                    KEY_OBJECT,
                    "--bucket-name",
                    KEY_BUCKET,
                    "--file",
                    str(encrypted),
                ],
                check=True,
            )
        with PUBLIC_KEY.open("x") as file:
            file.write(public + "\n")
    print(f"wrote the Sparkle public key to {PUBLIC_KEY}")


def generate_appcast(tool, dmg, output, build, seed):
    if not re.fullmatch(BUILD_NUMBER, build):
        raise ValueError("expected a positive macOS build number")
    with tempfile.TemporaryDirectory(dir=output.resolve().parent) as directory:
        staging = Path(directory)
        os.link(dmg, staging / dmg.name)
        subprocess.run(
            [
                tool,
                "--ed-key-file",
                "-",
                "--download-url-prefix",
                f"{PACKAGE_HOST}/{MACOS_PREFIX}/builds/{build}/",
                "--maximum-versions",
                "1",
                "--maximum-deltas",
                "0",
                staging,
            ],
            input=seed.rstrip() + b"\n",
            check=True,
        )
        (staging / APPCAST).replace(output)


def package(tool, dmg, output, build):
    expected = PUBLIC_KEY.read_text().strip()
    if len(base64.b64decode(expected, validate=True)) != 32:
        raise ValueError("invalid Sparkle public key")
    with signing_directory() as directory:
        encrypted = download_key(directory, KEY_OBJECT)
        seed = gpg(directory, passphrase("Sparkle"), "--decrypt", str(encrypted))
        _, public = key_pair(seed)
        if public != expected:
            raise ValueError("the R2 Sparkle key does not match the app's public key")
        generate_appcast(tool, dmg, output, build, seed)


if __name__ == "__main__":
    try:
        if sys.argv[1:] == ["bootstrap"]:
            bootstrap()
        elif len(sys.argv) == 5:
            package(
                Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4]
            )
        else:
            raise ValueError(
                "expected bootstrap or <tool> <dmg> <appcast> <build-number>"
            )
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        sys.exit(str(error))
