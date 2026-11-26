"""Publish a packaged stable release and update its package repositories."""

import base64
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen
from uuid import uuid4
from xml.etree import ElementTree

from sparkle import APPCAST, BUILD_NUMBER, MACOS_PREFIX, PACKAGE_HOST

RELEASE = Path("release")
AUR_REMOTE = "ssh://aur@aur.archlinux.org/tangramdotdev-bin.git"
AUR_FILES = (
    ".SRCINFO",
    "LICENSE",
    "PKGBUILD",
    "config.json",
    "tangram.service",
    "tangram.sysusers",
)
GITHUB_REPO = "tangramdotdev/tangram"
HOMEBREW_FORMULA = "repos/tangramdotdev/homebrew-tap/contents/tangram.rb"
PACKAGE_BUCKET = "pkgs-tangram-dev"
R2_LIMIT = 300 * 1024 * 1024
ARCHITECTURES = ("aarch64", "x86_64")
TARGETS = {
    f"{arch}-{platform}"
    for arch in ARCHITECTURES
    for platform in ("darwin", "linux-gnu", "linux-musl", "linux-musl-static")
}
HOMEBREW_ASSETS = {
    f"tangram_{arch}-{platform}.tar.gz"
    for arch in ARCHITECTURES
    for platform in ("darwin", "linux-gnu")
}
MACOS_APP = "Tangram.dmg"
MACOS_APPCAST = f"{MACOS_PREFIX}/{APPCAST}"
SPARKLE_NAMESPACE = "{http://www.andymatuschak.org/xml-namespaces/sparkle}"


def sha256_file(path):
    if path.is_symlink() or not path.is_file() or not path.stat().st_size:
        raise ValueError(f"expected a nonempty release file at {path}")
    with path.open("rb") as source:
        return hash_stream(source)


def hash_stream(stream, algorithm="sha256"):
    digest = hashlib.new(algorithm)
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        digest.update(chunk)
    return digest.hexdigest()


def atomic_write(path, data):
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as file:
        temporary = Path(file.name)
    try:
        temporary.write_bytes(data)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def release_manifest(version, targets):
    if not targets or not set(targets) <= TARGETS:
        raise ValueError("expected one or more supported release targets")
    checksums = {}

    def checksum(name):
        if name not in checksums:
            checksums[name] = sha256_file(RELEASE / name)
        return checksums[name]

    names = [f"tangram_{target}.tar.gz" for target in sorted(targets)]
    if any(target.endswith("-darwin") for target in targets):
        names.append(MACOS_APP)
    assets = {name: checksum(name) for name in names}

    # Build the upload plan for the repositories included in this package run.
    packages = {}
    immutable = {}
    indexes = {}
    heads = {}

    def add(group, name, *, key=None):
        source = RELEASE / name
        digest = checksum(name)
        if source.stat().st_size > R2_LIMIT:
            raise ValueError(f"{source} exceeds the cf R2 upload limit")
        group["stable/" + (key or name)] = {"file": name, "sha256": digest}

    gnu = [arch for arch in ARCHITECTURES if f"{arch}-linux-gnu" in targets]
    musl = [arch for arch in ARCHITECTURES if f"{arch}-linux-musl" in targets]
    for arch in gnu:
        deb = {"aarch64": "arm64", "x86_64": "amd64"}[arch]
        add(packages, f"apt/tangram_{version}_{deb}.deb")
        add(packages, f"rpm/tangram-{version}-1.{arch}.rpm")
    for arch in musl:
        add(packages, f"apk/{arch}/tangramdotdev-{version}-r0.apk")
        add(heads, f"apk/{arch}/APKINDEX.tar.gz")
    if musl:
        add(indexes, "apk/tangram-apk.rsa.pub")
    if gnu:
        for name in ("Packages", "Packages.gz"):
            path = f"apt/{name}"
            add(indexes, path)
            for algorithm in ("sha256", "sha512"):
                with (RELEASE / path).open("rb") as package_index:
                    digest = hash_stream(package_index, algorithm)
                add(immutable, path, key=f"apt/by-hash/{algorithm.upper()}/{digest}")
        add(indexes, "apt/Release")
        add(indexes, "apt/tangram-apt.asc")
        add(indexes, "rpm/RPM-GPG-KEY-tangram")
        for name in rpm_metadata():
            add(immutable, name)
        add(heads, "rpm/repodata/repomd.xml.asc")
        add(heads, "rpm/repodata/repomd.xml")
        add(heads, "apt/InRelease")
    aur = {}
    if gnu:
        aur = {name: checksum(f"aur/{name}") for name in AUR_FILES}
    macos = None
    if MACOS_APP in assets:
        appcast = (RELEASE / APPCAST).read_bytes()
        build, item = appcast_item(appcast)
        release_version = item.findtext(f"{SPARKLE_NAMESPACE}shortVersionString")
        enclosure = item.find("enclosure")
        key = f"{MACOS_PREFIX}/builds/{build}/{MACOS_APP}"
        if (
            release_version != version
            or enclosure is None
            or enclosure.get("url") != object_url(key)
            or enclosure.get("length") != str((RELEASE / MACOS_APP).stat().st_size)
            or not enclosure.get(f"{SPARKLE_NAMESPACE}edSignature")
        ):
            raise ValueError("the Sparkle appcast does not match the macOS release")
        add(immutable, MACOS_APP, key=key.removeprefix("stable/"))
        macos = {
            "build": build,
            "appcast": {"file": APPCAST, "sha256": checksum(APPCAST)},
        }
    return {
        "version": version,
        "targets": sorted(targets),
        "assets": assets,
        "aur": aur,
        "packages": packages,
        "immutable": immutable,
        "indexes": indexes,
        "heads": heads,
        "macos": macos,
    }


def appcast_item(data):
    if not re.search(
        rb"<!-- sparkle-signatures:\s+edSignature: [A-Za-z0-9+/]{86}==", data
    ):
        raise ValueError("the Sparkle appcast is missing its feed signature")
    try:
        (item,) = ElementTree.fromstring(data).findall("channel/item")
    except (ElementTree.ParseError, ValueError) as error:
        raise ValueError("expected one update in the Sparkle appcast") from error
    build = item.findtext(f"{SPARKLE_NAMESPACE}version") or ""
    if not re.fullmatch(BUILD_NUMBER, build):
        raise ValueError("invalid build number in the Sparkle appcast")
    return int(build), item


def rpm_metadata():
    xml = ElementTree.parse(RELEASE / "rpm/repodata/repomd.xml")
    entries = xml.findall("{*}data/{*}location")
    if not entries:
        raise ValueError("repomd.xml references no RPM metadata")
    paths = []
    for entry in entries:
        href = entry.get("href", "")
        if not re.fullmatch(r"repodata/[0-9a-f]{64}-[a-zA-Z0-9._-]+", href):
            raise ValueError(f"unexpected RPM metadata path: {href}")
        paths.append("rpm/" + href)
    return paths


def gh_release(tag):
    response = subprocess.run(
        [
            "gh",
            "release",
            "view",
            tag,
            "--repo",
            GITHUB_REPO,
            "--json",
            "databaseId,isDraft,isPrerelease",
        ],
        capture_output=True,
        text=True,
    )
    if not response.returncode:
        return json.loads(response.stdout)
    if response.stderr.strip() == "release not found":
        return None
    raise RuntimeError(
        response.stderr.strip() or "could not look up the GitHub release"
    )


def check_tag(tag):
    response = subprocess.run(
        ["gh", "api", f"repos/{GITHUB_REPO}/git/ref/tags/{tag}"],
        capture_output=True,
        text=True,
    )
    if response.returncode:
        raise ValueError(
            f"publishing requires the GitHub tag {tag}, which could not be found: "
            f"{response.stderr.strip() or 'gh api failed'}; push the tag, or pass "
            "--stage compile --stage package for a local build"
        )


def gh_assets(release):
    endpoint = (
        f"repos/{GITHUB_REPO}/releases/{release['databaseId']}/assets?per_page=100"
    )
    output = subprocess.check_output(["gh", "api", endpoint, "--paginate", "--slurp"])
    return {asset["name"]: asset for page in json.loads(output) for asset in page}


def publish_github(manifest):
    version = manifest["version"]
    tag = f"v{version}"
    release = gh_release(tag)
    if release is None:
        downloads = [*manifest["targets"]]
        if MACOS_APP in manifest["assets"]:
            downloads.append("Tangram.app disk image")
        notes = os.environ.get("TANGRAM_RELEASE_NOTES") or (
            "Downloads\n\n" + "\n".join(f"- {name}" for name in downloads)
        )
        subprocess.run(
            [
                "gh",
                "release",
                "create",
                tag,
                "--repo",
                GITHUB_REPO,
                "--verify-tag",
                "--draft",
                "--title",
                tag,
                "--notes-file",
                "-",
            ],
            input=notes,
            text=True,
            check=True,
        )
        release = gh_release(tag)
        if release is None:
            raise RuntimeError(f"could not find the newly created GitHub release {tag}")
    assets = gh_assets(release)
    selected = manifest["assets"]
    published = selected.copy()
    for name in [
        MACOS_APP,
        *(f"tangram_{target}.tar.gz" for target in sorted(TARGETS)),
    ]:
        if name in selected or name not in assets:
            continue
        digest = assets[name].get("digest") or ""
        if not re.fullmatch(r"sha256:[a-f0-9]{64}", digest):
            raise ValueError(f"GitHub asset {tag}/{name} has no SHA-256 digest")
        published[name] = digest.removeprefix("sha256:")

    pending = [
        str(RELEASE / name)
        for name, digest in selected.items()
        if assets.get(name, {}).get("digest") != f"sha256:{digest}"
    ]
    if pending:
        upload_github_assets(tag, pending)
    assets = gh_assets(release)
    for name, digest in published.items():
        asset = assets.get(name, {})
        if (
            asset.get("state") != "uploaded"
            or asset.get("digest") != f"sha256:{digest}"
        ):
            raise RuntimeError(f"could not verify the GitHub asset {tag}/{name}")
    if release["isDraft"] or release["isPrerelease"]:
        subprocess.run(
            [
                "gh",
                "release",
                "edit",
                tag,
                "--repo",
                GITHUB_REPO,
                "--draft=false",
                "--prerelease=false",
                "--latest",
            ],
            check=True,
        )
    return published


def upload_github_assets(tag, files):
    subprocess.run(
        ["gh", "release", "upload", tag, *files, "--repo", GITHUB_REPO, "--clobber"],
        check=True,
    )


def object_url(key):
    return f"{PACKAGE_HOST}/{quote(key, safe='/')}"


def public_response(key, nonce=None):
    url = object_url(key)
    if nonce is not None:
        url += f"?publish={nonce}"
    request = Request(
        url,
        headers={"Accept-Encoding": "identity", "User-Agent": "Tangram-Release/1.0"},
    )
    try:
        return urlopen(request, timeout=30)
    except HTTPError as error:
        if error.code != 404:
            raise
        return None


def public_checksum(key, nonce=None):
    response = public_response(key, nonce)
    if response is None:
        return None
    with response:
        return hash_stream(response)


def check_macos_update(update):
    response = public_response(MACOS_APPCAST, uuid4().hex)
    if response is None:
        return
    with response:
        current = response.read()
    build, _ = appcast_item(current)
    if (
        build >= update["build"]
        and hashlib.sha256(current).hexdigest() != update["appcast"]["sha256"]
    ):
        raise ValueError(f"the stable Sparkle feed already publishes build {build}")


def content_type(name):
    if name.endswith(".gz"):
        return "application/gzip"
    if name.endswith(".asc"):
        return "application/pgp-signature"
    if name.endswith(".xml"):
        return "application/xml"
    if name.endswith(".dmg"):
        return "application/x-apple-diskimage"
    return "application/octet-stream"


def pending_r2_objects(objects, *, immutable=False):
    pending = {}
    for key, entry in objects.items():
        expected = entry["sha256"]
        current = public_checksum(key, uuid4().hex)
        if current == expected:
            continue
        if immutable and current is not None:
            raise ValueError(
                f"existing R2 object differs from the packaged release: {key}"
            )
        pending[key] = entry
    return pending


def upload_r2_objects(objects, *, verify=False):
    for key, entry in objects.items():
        path = RELEASE / entry["file"]
        subprocess.run(
            [
                "cf",
                "r2",
                "objects",
                "put",
                key,
                "--bucket-name",
                PACKAGE_BUCKET,
                "--file",
                str(path),
                "--content-type",
                content_type(path.name),
            ],
            check=True,
        )
        if verify and public_checksum(key, uuid4().hex) != entry["sha256"]:
            raise RuntimeError(f"could not verify the uploaded R2 object: {key}")


def publish_cached_objects(objects):
    if not objects:
        return
    upload_r2_objects(pending_r2_objects(objects))
    subprocess.run(
        [
            "cf",
            "cache",
            "purge",
            "--zone",
            "tangram.dev",
            "--force",
            "--body",
            json.dumps({"files": [object_url(key) for key in objects]}),
        ],
        check=True,
    )
    for key, entry in objects.items():
        if public_checksum(key) != entry["sha256"]:
            raise RuntimeError(f"could not verify the repository file: {key}")


def update_homebrew(manifest):
    response = subprocess.check_output(["gh", "api", f"{HOMEBREW_FORMULA}?ref=main"])
    current = json.loads(response)
    formula = base64.b64decode(current["content"]).decode()
    updated = homebrew_formula(formula, manifest)
    if updated == formula:
        return
    body = {
        "branch": "main",
        "content": base64.b64encode(updated.encode()).decode(),
        "message": f"release tangram {manifest['version']}",
        "sha": current["sha"],
    }
    subprocess.run(
        ["gh", "api", "--method", "PUT", HOMEBREW_FORMULA, "--input", "-"],
        input=json.dumps(body).encode(),
        stdout=subprocess.DEVNULL,
        check=True,
    )
    print(f"updated the Homebrew tap for Tangram {manifest['version']}", flush=True)


def homebrew_formula(formula, manifest):
    version = manifest["version"]
    version_line = r'^([ \t]*version ")[^"\n]+(")$'
    formula, count = re.subn(
        version_line, rf"\g<1>{version}\g<2>", formula, flags=re.MULTILINE
    )
    if count != 1:
        raise ValueError("expected one version in the Homebrew formula")
    for architecture in ("aarch64", "x86_64"):
        for platform in ("darwin", "linux-gnu"):
            filename = f"tangram_{architecture}-{platform}.tar.gz"
            # The canary formula used the Linux triple without a libc suffix.
            legacy = filename.replace("-linux-gnu", "-linux")
            names = "|".join(re.escape(name) for name in {filename, legacy})
            url_line = rf'(^[ \t]*url ")[^"\n]+/(?:{names})("\n[ \t]+sha256 ")[^"]*(")'
            url = f"https://github.com/{GITHUB_REPO}/releases/download/v{version}/{filename}"
            checksum = manifest["assets"][filename]
            formula, count = re.subn(
                url_line,
                rf"\g<1>{url}\g<2>{checksum}\g<3>",
                formula,
                flags=re.MULTILINE,
            )
            if count != 1:
                raise ValueError(
                    f"expected one Homebrew URL and checksum for {filename}"
                )
    return formula


def write_recipe(directory, version, archives):
    # Write the AUR recipe and its source information in the sandboxed packaging build.
    arg = json.dumps({"archives": archives, "version": version}, separators=(",", ":"))
    with tempfile.TemporaryDirectory(prefix="tangram-aur-") as temporary:
        output = Path(temporary) / "aur"
        subprocess.run(
            [
                "tg",
                "build",
                "./packages/release/build.tg.ts#aur",
                "--arg-value",
                arg,
                f"--checkout={output}",
            ],
            check=True,
        )
        for name in ("PKGBUILD", ".SRCINFO"):
            atomic_write(directory / name, (output / name).read_bytes())


def update_aur(manifest, assets):
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    env.setdefault("GIT_SSH_COMMAND", "ssh -o BatchMode=yes -o ConnectTimeout=10")
    version = manifest["version"]
    archives = {}
    for arch in ARCHITECTURES:
        asset = f"tangram_{arch}-linux-gnu.tar.gz"
        if asset in assets:
            archives[arch] = assets[asset]
    extend_recipe = any(
        f"{arch}-linux-gnu" not in manifest["targets"] for arch in archives
    )
    with tempfile.TemporaryDirectory(prefix="tangram-aur-") as directory:
        checkout = Path(directory)

        def git(*args, check=True):
            return subprocess.run(
                ["git", *args], cwd=checkout, env=env, check=check, timeout=120
            )

        git("-c", "init.defaultBranch=master", "clone", "--no-tags", AUR_REMOTE, ".")
        for name, digest in manifest["aur"].items():
            data = (RELEASE / "aur" / name).read_bytes()
            if hashlib.sha256(data).hexdigest() != digest:
                raise ValueError(f"AUR release file changed after packaging: {name}")
            atomic_write(checkout / name, data)
        if extend_recipe:
            write_recipe(checkout, version, archives)
        git("add", "--", *AUR_FILES)
        status = git("diff", "--cached", "--quiet", "--", check=False)
        if status.returncode == 0:
            return
        if status.returncode != 1:
            status.check_returncode()
        git("commit", "-m", f"release tangram {version}")
        git("push", "origin", "HEAD:refs/heads/master")
    print(f"updated the AUR package for Tangram {version}", flush=True)


def main(action, version, targets):
    if not re.fullmatch(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", version):
        raise ValueError("expected a numeric release version")
    manifest_path = RELEASE / "manifest.json"
    if action == "check":
        check_tag(f"v{version}")
        return
    if action == "manifest":
        manifest = release_manifest(version, targets)
        atomic_write(manifest_path, (json.dumps(manifest, indent=2) + "\n").encode())
        return
    if action != "publish":
        raise ValueError("expected check, manifest, or publish")
    if not manifest_path.is_file():
        raise ValueError("release/manifest.json is missing; run signed packaging")
    prepared = json.loads(manifest_path.read_bytes())
    if prepared.get("version") != version:
        raise ValueError("publish version must match the signed package manifest")
    packaged_targets = prepared.get("targets", [])
    if targets and sorted(targets) != packaged_targets:
        raise ValueError("publish targets must match the signed package manifest")
    manifest = release_manifest(version, packaged_targets)
    if manifest != prepared:
        raise ValueError("release files or version changed after signed packaging")

    macos = manifest["macos"]
    if macos:
        check_macos_update(macos)
    pending = pending_r2_objects(manifest["immutable"], immutable=True)
    assets = publish_github(manifest)
    upload_r2_objects(pending, verify=True)
    publish_cached_objects(manifest["packages"])
    publish_cached_objects(manifest["indexes"])
    publish_cached_objects(manifest["heads"])
    if HOMEBREW_ASSETS <= assets.keys():
        update_homebrew({**manifest, "assets": assets})
    if manifest["aur"]:
        update_aur(manifest, assets)
    if macos:
        check_macos_update(macos)
        publish_cached_objects({MACOS_APPCAST: macos["appcast"]})


if __name__ == "__main__":
    try:
        main(sys.argv[1], sys.argv[2], sys.argv[3:])
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        sys.exit(str(error))
