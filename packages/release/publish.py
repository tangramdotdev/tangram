"""Publish the targets of a stable release and update the indexes over all of them."""

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
IMAGE_REPOSITORY = "ghcr.io/tangramdotdev/tangram"
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
DEB_ARCHITECTURES = {"aarch64": "arm64", "x86_64": "amd64"}
MACOS_APP = "Tangram.dmg"
MACOS_APPCAST = f"{MACOS_PREFIX}/{APPCAST}"
SPARKLE_NAMESPACE = "{http://www.andymatuschak.org/xml-namespaces/sparkle}"
PUBLISHED = "published.json"
RECORD = "record.json"
PACKAGE_PATH = r"(apt|rpm|apk/(aarch64|x86_64))/[A-Za-z0-9._~+-]+\.(deb|rpm|apk)"


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


def release_manifest(version, targets, published):
    if not targets or not set(targets) <= TARGETS:
        raise ValueError("expected one or more supported release targets")
    commit = staged_commit(targets)
    checksums = {}

    def checksum(name):
        if name not in checksums:
            checksums[name] = sha256_file(RELEASE / name)
        return checksums[name]

    assets = {
        name: checksum(name)
        for target in sorted(targets)
        for name in target_assets(target)
    }

    # Build the upload plan for the repositories included in this package run.
    packages = {}
    immutable = {}
    indexes = {}
    heads = {}
    images = {}
    records = {}

    def add(group, name, *, key=None):
        source = RELEASE / name
        digest = checksum(name)
        if source.stat().st_size > R2_LIMIT:
            raise ValueError(f"{source} exceeds the cf R2 upload limit")
        group["stable/" + (key or name)] = {"file": name, "sha256": digest}

    gnu = [arch for arch in ARCHITECTURES if f"{arch}-linux-gnu" in targets]
    musl = [arch for arch in ARCHITECTURES if f"{arch}-linux-musl" in targets]

    # Describe what each target of this run publishes, which the release's record keeps.
    for target in sorted(targets):
        arch = target.split("-")[0]
        record = {"assets": {name: assets[name] for name in target_assets(target)}}
        files = []
        if target.endswith("-linux-gnu"):
            deb = DEB_ARCHITECTURES[arch]
            files = [
                f"apt/tangram_{version}_{deb}.deb",
                f"rpm/tangram-{version}-1.{arch}.rpm",
            ]
            image = f"image/tangram_{version}_{deb}.tar"
            record["image"] = f"{IMAGE_REPOSITORY}:{version}-{deb}"
            images[record["image"]] = {"file": image, "sha256": checksum(image)}
        elif target.endswith("-linux-musl"):
            files = [f"apk/{arch}/tangramdotdev-{version}-r0.apk"]
        for name in files:
            add(packages, name)
        record["packages"] = {name: checksum(name) for name in files}
        records[target] = record

    # Plan the repository metadata.
    for arch in musl:
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
        "commit": commit,
        "targets": sorted(targets),
        "assets": assets,
        "aur": aur,
        "packages": packages,
        "images": images,
        "immutable": immutable,
        "indexes": indexes,
        "heads": heads,
        "macos": macos,
        "records": records,
        "published": published,
    }


def target_assets(target):
    names = [f"tangram_{target}.tar.gz"]
    if target.endswith("-darwin"):
        names.append(MACOS_APP)
    return names


def staged_commit(targets):
    commits = {
        json.loads((RELEASE / f"tangram_{target}.json").read_bytes()).get("commit")
        for target in targets
    }
    if len(commits) != 1 or not isinstance(next(iter(commits)), str):
        raise ValueError("expected the staged targets to share one source commit")
    return commits.pop()


def source_commit():
    output = subprocess.check_output([sys.executable, "scripts/version.py"], text=True)
    return output.strip()


def record_key(version):
    return f"stable/releases/{version}.json"


def published_record(version, commit):
    # Read the record of what earlier runs published, and check its commit.
    response = public_response(record_key(version), uuid4().hex)
    if response is None:
        return None
    with response:
        record = json.loads(response.read())
    if record.get("version") != version or not isinstance(record.get("targets"), dict):
        raise ValueError(f"the record of release {version} is invalid")
    if record.get("commit") != commit:
        raise ValueError(
            f"release {version} was published from commit {record.get('commit')}, "
            f"so this run from {commit} cannot add to it"
        )
    return record


def other_targets(record, targets):
    if record is None:
        return {}
    return {
        target: entry
        for target, entry in sorted(record["targets"].items())
        if target not in targets
    }


def gnu_targets(entries):
    return {
        target: entry
        for target, entry in entries.items()
        if target.endswith("-linux-gnu")
    }


def merge_targets(others, records):
    # Give each asset of this run, such as the macOS app, to this run's targets.
    names = {name for record in records.values() for name in record["assets"]}
    merged = {
        target: {
            **entry,
            "assets": {
                name: digest
                for name, digest in entry["assets"].items()
                if name not in names
            },
        }
        for target, entry in others.items()
    }
    return dict(sorted({**merged, **records}.items()))


def fetch(version, targets):
    # Download the GNU Linux packages of other runs, which the metadata covers.
    commit = staged_commit(targets)
    others = other_targets(published_record(version, commit), targets)
    if any(target.endswith("-linux-gnu") for target in targets):
        for entry in gnu_targets(others).values():
            for name, digest in entry["packages"].items():
                download_package(name, digest)
    data = json.dumps(others, indent=2, sort_keys=True) + "\n"
    atomic_write(RELEASE / PUBLISHED, data.encode())


def download_package(name, digest):
    if not re.fullmatch(PACKAGE_PATH, name):
        raise ValueError(f"unexpected published package path: {name}")
    key = f"stable/{name}"
    response = public_response(key, uuid4().hex)
    if response is None:
        raise RuntimeError(f"the published package is missing: {key}")
    with response:
        data = response.read()
    if hashlib.sha256(data).hexdigest() != digest:
        raise RuntimeError(f"the published package does not match its record: {key}")
    path = RELEASE / name
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write(path, data)


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


def publish_github(manifest, targets, assets):
    version = manifest["version"]
    tag = f"v{version}"
    downloads = [*targets]
    if MACOS_APP in assets:
        downloads.append("Tangram.app disk image")
    notes = os.environ.get("TANGRAM_RELEASE_NOTES") or (
        "Downloads\n\n" + "\n".join(f"- {name}" for name in downloads)
    )
    release = gh_release(tag)
    if release is None:
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

    # Upload this run's assets, and verify every asset that the release has published.
    uploaded = gh_assets(release)
    pending = [
        str(RELEASE / name)
        for name, digest in manifest["assets"].items()
        if uploaded.get(name, {}).get("digest") != f"sha256:{digest}"
    ]
    if pending:
        upload_github_assets(tag, pending)
    uploaded = gh_assets(release)
    for name, digest in assets.items():
        asset = uploaded.get(name, {})
        if (
            asset.get("state") != "uploaded"
            or asset.get("digest") != f"sha256:{digest}"
        ):
            raise RuntimeError(f"could not verify the GitHub asset {tag}/{name}")

    # Publish the release with notes that list every published target.
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
            "--notes-file",
            "-",
        ],
        input=notes,
        text=True,
        check=True,
    )


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
    if name.endswith(".json"):
        return "application/json"
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


def push_images(images):
    for tag, entry in images.items():
        path = RELEASE / entry["file"]
        if sha256_file(path) != entry["sha256"]:
            raise ValueError(f"the container image changed after packaging: {path}")
        subprocess.run(["docker", "load", "--input", str(path)], check=True)
        subprocess.run(["docker", "push", tag], check=True)


def publish_image_index(version, targets):
    tags = sorted(entry["image"] for entry in targets.values() if "image" in entry)
    subprocess.run(
        [
            "docker",
            "buildx",
            "imagetools",
            "create",
            "--tag",
            f"{IMAGE_REPOSITORY}:{version}",
            "--tag",
            f"{IMAGE_REPOSITORY}:latest",
            *tags,
        ],
        check=True,
    )
    print(f"published the container image of Tangram {version}", flush=True)


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
        published_record(version, source_commit())
        return
    if action == "fetch":
        fetch(version, targets)
        return
    if action == "manifest":
        published = json.loads((RELEASE / PUBLISHED).read_bytes())
        manifest = release_manifest(version, targets, published)
        atomic_write(manifest_path, (json.dumps(manifest, indent=2) + "\n").encode())
        return
    if action != "publish":
        raise ValueError("expected check, fetch, manifest, or publish")
    if not manifest_path.is_file():
        raise ValueError("release/manifest.json is missing; run signed packaging")
    prepared = json.loads(manifest_path.read_bytes())
    if prepared.get("version") != version:
        raise ValueError("publish version must match the signed package manifest")
    packaged_targets = prepared.get("targets", [])
    if targets and sorted(targets) != packaged_targets:
        raise ValueError("publish targets must match the signed package manifest")
    manifest = release_manifest(version, packaged_targets, prepared.get("published"))
    if manifest != prepared:
        raise ValueError("release files or version changed after signed packaging")

    # Merge the targets of other runs, whose GNU Linux packages the metadata covers.
    record = published_record(version, manifest["commit"])
    others = other_targets(record, packaged_targets)
    covered = gnu_targets(manifest["published"])
    if gnu_targets(manifest["records"]) and gnu_targets(others) != covered:
        raise ValueError(
            f"other runs published GNU Linux packages of release {version} "
            "after packaging; package again"
        )
    targets = merge_targets(others, manifest["records"])
    assets = {
        name: digest
        for entry in targets.values()
        for name, digest in entry["assets"].items()
    }

    # Publish this run's targets, and record them with the release.
    macos = manifest["macos"]
    if macos:
        check_macos_update(macos)
    pending = pending_r2_objects(manifest["immutable"], immutable=True)
    publish_github(manifest, targets, assets)
    upload_r2_objects(pending, verify=True)
    publish_cached_objects(manifest["packages"])
    push_images(manifest["images"])
    record = {"commit": manifest["commit"], "targets": targets, "version": version}
    data = json.dumps(record, indent=2, sort_keys=True) + "\n"
    atomic_write(RELEASE / RECORD, data.encode())
    publish_cached_objects(
        {record_key(version): {"file": RECORD, "sha256": sha256_file(RELEASE / RECORD)}}
    )

    # Update the indexes over every published target.
    publish_cached_objects(manifest["indexes"])
    publish_cached_objects(manifest["heads"])
    if manifest["images"]:
        publish_image_index(version, targets)
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
