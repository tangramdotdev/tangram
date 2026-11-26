"""Stand in for the cf, gh, and docker commands of a publish, and serve the bucket."""

import hashlib
import json
import os
import shutil
import sys
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def main():
    state = Path(sys.argv[1])
    command = sys.argv[2]
    args = sys.argv[3:]
    commands = {"cf": cf, "docker": docker, "gh": gh, "serve": serve}
    commands[command](state, args)


def load(path, default):
    return json.loads(path.read_text()) if path.is_file() else default


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def option(args, name):
    return args[args.index(name) + 1]


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def fail(message):
    print(message, file=sys.stderr)
    sys.exit(1)


def cf(state, args):
    # Put an object in the bucket, and accept a cache purge.
    if args[:3] == ["r2", "objects", "put"]:
        destination = state / "bucket" / args[3]
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(option(args, "--file"), destination)
    elif args[:2] != ["cache", "purge"]:
        fail(f"unexpected cf command: {args}")


def docker(state, args):
    # Load an image whose archive is its tag, push it, and index pushed images.
    path = state / "registry.json"
    registry = load(path, {"indexes": {}, "loaded": {}, "pushed": {}})
    if args[:2] == ["load", "--input"]:
        tag = Path(args[2]).read_text().strip()
        registry["loaded"][tag] = sha256(args[2])
        print(f"Loaded image: {tag}")
    elif args[:1] == ["push"]:
        if args[1] not in registry["loaded"]:
            fail(f"no loaded image {args[1]}")
        registry["pushed"][args[1]] = registry["loaded"][args[1]]
    elif args[:3] == ["buildx", "imagetools", "create"]:
        tags = []
        sources = []
        rest = iter(args[3:])
        for arg in rest:
            if arg == "--tag":
                tags.append(next(rest))
            else:
                sources.append(arg)
        for source in sources:
            if source not in registry["pushed"]:
                fail(f"no pushed image {source}")
        for tag in tags:
            registry["indexes"][tag] = sources
    else:
        fail(f"unexpected docker command: {args}")
    save(path, registry)


def gh(state, args):
    # Act on the GitHub releases, every tag of which exists.
    path = state / "github.json"
    releases = load(path, {})
    if args[0] == "api":
        endpoint = args[1]
        if "/git/ref/tags/" in endpoint:
            print("{}")
            return
        if endpoint.startswith("repos/tangramdotdev/tangram/releases/"):
            identifier = int(endpoint.split("/")[4])
            (release,) = [
                release
                for release in releases.values()
                if release["databaseId"] == identifier
            ]
            assets = [
                {"name": name, **asset}
                for name, asset in sorted(release["assets"].items())
            ]
            print(json.dumps([assets]))
            return
        fail(f"not found: {endpoint}")
    tag = args[2]
    action = args[1]
    if action == "view":
        if tag not in releases:
            fail("release not found")
        release = releases[tag]
        print(
            json.dumps(
                {key: release[key] for key in ("databaseId", "isDraft", "isPrerelease")}
            )
        )
        return
    if action == "create":
        releases[tag] = {
            "assets": {},
            "databaseId": len(releases) + 1,
            "isDraft": "--draft" in args,
            "isPrerelease": False,
            "notes": sys.stdin.read(),
        }
    elif action == "upload":
        files = args[3 : args.index("--repo")]
        directory = state / "assets" / tag
        directory.mkdir(parents=True, exist_ok=True)
        for file in files:
            name = Path(file).name
            shutil.copyfile(file, directory / name)
            releases[tag]["assets"][name] = {
                "digest": f"sha256:{sha256(file)}",
                "state": "uploaded",
            }
    elif action == "edit":
        release = releases[tag]
        release["isDraft"] = release["isDraft"] and "--draft=false" not in args
        release["isPrerelease"] = (
            release["isPrerelease"] and "--prerelease=false" not in args
        )
        if "--notes-file" in args:
            release["notes"] = sys.stdin.read()
    else:
        fail(f"unexpected gh command: {args}")
    save(path, releases)


class Handler(SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass


def serve(state, args):
    # Serve the bucket, write the port, and signal readiness to the supervisor.
    bucket = state / "bucket"
    bucket.mkdir(parents=True, exist_ok=True)
    handler = partial(Handler, directory=str(bucket))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    (state / "port").write_text(str(server.server_address[1]))
    os.write(3, b"\0")
    os.close(3)
    server.serve_forever()


if __name__ == "__main__":
    main()
