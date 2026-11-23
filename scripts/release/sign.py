"""Prepare signed Linux repositories from the staged packages."""

import getpass
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import unquote_to_bytes

KEY_BUCKET = "keys-tangram-dev"
PUBLIC_KEYS = Path("packages/linux/keys").resolve()
APT_SIGNER = "6CA7EC3F820AD62E65DD5D1B7BDC43714473D2A5"
RPM_SIGNER = "09C7C2B1A3F349390984A7D148E333B80D02A7E8"
APT_IMAGE = """\
FROM debian:trixie-slim
RUN apt-get update -qq && apt-get install -yq --no-install-recommends apt-utils
"""
RPM_IMAGE = (
    "FROM fedora:45\n"
    "RUN dnf -qy --setopt=install_weak_deps=False install "
    "rpm-sign createrepo_c gnupg2 && dnf clean all\n"
)
APK_IMAGE = """\
FROM alpine:3.22
RUN apk add --no-cache abuild tar
"""


def main(version, targets):
    release = Path("release").resolve()
    repositories = {
        "apt": [
            release / "apt" / f"tangram_{version}_{architecture}.deb"
            for target, architecture in [
                ("aarch64-linux-gnu", "arm64"),
                ("x86_64-linux-gnu", "amd64"),
            ]
            if target in targets
        ],
        "rpm": [
            release
            / "rpm"
            / f"tangram-{version.replace('-', '~')}-1.{architecture}.rpm"
            for architecture in ("aarch64", "x86_64")
            if f"{architecture}-linux-gnu" in targets
        ],
        "apk": [
            release / "apk" / architecture / apk_name(version)
            for architecture in ("aarch64", "x86_64")
            if f"{architecture}-linux-musl" in targets
        ],
    }
    if not any(repositories.values()):
        return

    with signing_directory() as secrets:
        with tempfile.TemporaryDirectory(prefix=".signing-", dir=release) as staging:
            stage = Path(staging)
            for name, files in repositories.items():
                if not files:
                    continue
                repo = stage / name
                for path in files:
                    destination = stage / path.relative_to(release)
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(path, destination)
                print(f"Preparing signed {name.upper()} repository.", flush=True)
                if name == "apt":
                    sign_apt(repo, secrets)
                elif name == "rpm":
                    sign_rpm(repo, secrets)
                elif name == "apk":
                    sign_apk(repo, secrets)

            # Place verified files before the indexes and their signatures.
            outputs = sorted(
                (path for path in stage.rglob("*") if path.is_file()),
                key=lambda path: path.suffix not in (".deb", ".rpm", ".apk"),
            )
            for path in outputs:
                destination = release / path.relative_to(stage)
                destination.parent.mkdir(parents=True, exist_ok=True)
                path.replace(destination)


def apk_name(version):
    version = re.sub(r"-(alpha|beta|pre|rc)\.?([0-9]*)$", r"_\1\2", version)
    return f"tangramdotdev-{version}-r0.apk"


@contextmanager
def signing_directory():
    keys = Path("keys")
    keys.mkdir(mode=0o700, exist_ok=True)
    info = keys.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_mode & 0o022
    ):
        raise RuntimeError("keys/ must be a directory owned and writable only by you")
    with tempfile.TemporaryDirectory(
        prefix=".signing-", dir=keys.resolve()
    ) as directory:
        yield Path(directory)


def download_key(directory, name):
    key = directory / name
    descriptor = os.open(key, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as file:
        subprocess.run(
            ["cf", "r2", "objects", "get", name, "--bucket-name", KEY_BUCKET],
            stdout=file,
            check=True,
        )
    if not key.stat().st_size:
        raise RuntimeError(f"downloaded key is empty: {name}")
    return key


def passphrase(label):
    if sys.platform == "darwin" and shutil.which("pinentry-mac"):
        commands = (
            f"SETDESC Paste the Tangram {label} signing passphrase.\nGETPIN\nBYE\n"
        )
        result = subprocess.run(
            ["pinentry-mac"], input=commands.encode(), capture_output=True, check=True
        )
        secret = next(
            (
                unquote_to_bytes(line[2:])
                for line in result.stdout.splitlines()
                if line.startswith(b"D ")
            ),
            b"",
        )
    else:
        with open("/dev/tty", "w") as terminal:
            secret = getpass.getpass(
                f"Tangram {label} signing passphrase: ", stream=terminal
            ).encode()
    if not secret or b"\n" in secret or b"\r" in secret:
        raise RuntimeError(f"expected a passphrase for {label} signing")
    return secret + b"\n"


def container(dockerfile, repo, command, *, key=None, password=None):
    image = (
        subprocess.run(
            ["docker", "build", "--quiet", "-"],
            input=dockerfile.encode(),
            stdout=subprocess.PIPE,
            check=True,
        )
        .stdout.decode()
        .strip()
    )
    args = [
        "docker",
        "run",
        "--rm",
        "--network=none",
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "--volume",
        f"{repo}:/repo",
        "--workdir",
        "/repo",
    ]
    if key is not None:
        args += ["--volume", f"{key}:/signing-key:ro", "--tmpfs", "/tmp", "-i"]
    subprocess.run(args + [image, "sh", "-ec", command], input=password, check=True)


def sign_apt(repo, secrets):
    key = download_key(secrets, "apt-signing-subkey.asc")
    public = PUBLIC_KEYS / "tangram-apt.asc"
    container(
        APT_IMAGE,
        repo,
        """
        apt-ftparchive packages . > Packages
        gzip -nc Packages > Packages.gz
        apt-ftparchive -o APT::FTPArchive::Release::Origin=Tangram \
            -o APT::FTPArchive::Release::Acquire-By-Hash=yes \
            -o APT::FTPArchive::Release::Suite=stable release . > /tmp/Release
        cp /tmp/Release Release
        """,
    )
    home = secrets / "gnupg"
    home.mkdir(mode=0o700)
    gpg = ["gpg", "--homedir", str(home), "--batch", "--no-tty"]
    keyring = secrets / "apt-public.gpg"
    try:
        subprocess.run(gpg + ["--import", str(key)], check=True)
        subprocess.run(
            gpg
            + [
                "--pinentry-mode",
                "loopback",
                "--passphrase-fd",
                "0",
                "--digest-algo",
                "SHA256",
                "--local-user",
                APT_SIGNER + "!",
                "--output",
                str(repo / "InRelease"),
                "--clearsign",
                str(repo / "Release"),
            ],
            input=passphrase("APT"),
            check=True,
        )
        subprocess.run(
            gpg + ["--output", str(keyring), "--dearmor", str(public)], check=True
        )
        subprocess.run(
            [
                "gpgv",
                "--homedir",
                str(home),
                "--keyring",
                str(keyring),
                str(repo / "InRelease"),
            ],
            check=True,
        )
    finally:
        subprocess.run(
            ["gpgconf", "--homedir", str(home), "--kill", "gpg-agent"], check=False
        )
    shutil.copyfile(public, repo / public.name)


def sign_rpm(repo, secrets):
    key = download_key(secrets, "rpm-signing.asc")
    public = PUBLIC_KEYS / "RPM-GPG-KEY-tangram"
    shutil.copyfile(public, repo / public.name)
    container(
        RPM_IMAGE,
        repo,
        f"""
        export GNUPGHOME=$(mktemp -d)
        echo allow-preset-passphrase > "$GNUPGHOME/gpg-agent.conf"
        gpg --batch --import /signing-key
        grip=$(gpg --batch --with-colons --with-keygrip -K {RPM_SIGNER} |
            awk -F: '$1 == "grp" {{ print $10; exit }}')
        test -n "$grip"
        gpg-connect-agent /bye > /dev/null
        /usr/libexec/gpg-preset-passphrase --preset "$grip"
        rpmsign --define '_openpgp_sign gpg' --define '_gpg_digest_algo sha256' \
            --define '_openpgp_sign_id {RPM_SIGNER}!' --resign --rpmv4 ./*.rpm
        rpmkeys --dbpath "$GNUPGHOME/rpmdb" --import RPM-GPG-KEY-tangram
        rpmkeys --dbpath "$GNUPGHOME/rpmdb" --checksig ./*.rpm
        createrepo_c .
        gpg --batch --local-user {RPM_SIGNER}! --digest-algo SHA256 --armor \
            --detach-sign repodata/repomd.xml
        gpg --batch --verify repodata/repomd.xml.asc repodata/repomd.xml
        """,
        key=key,
        password=passphrase("RPM"),
    )


def sign_apk(repo, secrets):
    key = download_key(secrets, "tangram-apk.rsa")
    public = PUBLIC_KEYS / "tangram-apk.rsa.pub"
    shutil.copyfile(public, repo / public.name)
    container(
        APK_IMAGE,
        repo,
        """
        umask 077
        work=$(mktemp -d)
        openssl pkey -in /signing-key -passin stdin -out "$work/key"
        openssl pkey -in "$work/key" -pubout -out "$work/tangram-apk.rsa.pub"
        cmp "$work/tangram-apk.rsa.pub" /repo/tangram-apk.rsa.pub
        for architecture in /repo/*/; do
            for package in "$architecture"*.apk; do
                (
                    cd "$work"
                    abuild-gzsplit < "$package"
                    abuild-sign -t RSA256 -k key -p tangram-apk.rsa.pub control.tar.gz
                    cat control.tar.gz data.tar.gz > "$package"
                )
            done
            cd "$architecture"
            arch=$(basename "$architecture")
            apk_repo() {
                apk --repositories-file /dev/null --keys-dir /repo --arch "$arch" "$@"
            }
            apk_repo verify ./*.apk
            apk_repo index --no-warnings --output APKINDEX.tar.gz ./*.apk
            abuild-sign -t RSA256 -k "$work/key" -p tangram-apk.rsa.pub APKINDEX.tar.gz
            apk_repo --repository /repo search --exact tangramdotdev |
                grep -q '^tangramdotdev-'
        done
        """,
        key=key,
        password=passphrase("Alpine"),
    )


if __name__ == "__main__":
    try:
        main(sys.argv[1], sys.argv[2:])
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        sys.exit(str(error))
