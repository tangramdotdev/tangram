"""Embedded client bootstrap and filesystem Tangram module imports."""

from __future__ import annotations

import asyncio
import importlib.abc
import importlib.machinery
import importlib.util
import inspect
import json
import linecache
import posixpath
import sys
import traceback
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType
from typing import Any, Protocol


class Host(Protocol):
    def inventory(self, module: str) -> str: ...

    def module(self, referrer: str, reference: str | None) -> tuple[str, str]: ...


class Embedded(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def __init__(self, sources: dict[str, list[Any]]):
        self.sources = sources

    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None = None,
        target: ModuleType | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        source = self.sources.get(fullname)
        if source is None:
            return None
        return importlib.util.spec_from_loader(fullname, self, is_package=source[1])

    def create_module(self, spec: importlib.machinery.ModuleSpec) -> ModuleType | None:
        return None

    def exec_module(self, module: ModuleType) -> None:
        text, _ = self.sources[module.__name__]
        filename = f"<embedded {module.__name__}>"
        module.__file__ = filename
        linecache.cache[filename] = (len(text), None, text.splitlines(True), filename)
        exec(compile(text, filename, "exec"), module.__dict__)


class Loader(importlib.abc.Loader):
    def __init__(
        self,
        finder: Finder,
        data: dict[str, Any],
        text: str,
        package: bool,
        filename: str,
    ) -> None:
        self.finder = finder
        self.data = data
        self.text = text
        self.package = package
        self.filename = filename
        identity = json.loads(json.dumps(data))
        identity["referent"].get("options", {}).pop("tokens", None)
        self.identity = json.dumps(identity, sort_keys=True)

    def create_module(self, spec: importlib.machinery.ModuleSpec) -> ModuleType | None:
        return self.finder.modules.get(self.identity)

    def exec_module(self, module: ModuleType) -> None:
        if self.identity in self.finder.modules:
            return
        self.finder.modules[self.identity] = module
        filename = self.filename
        module.__file__ = filename
        module.__dict__["tg"] = self.finder.tg
        setattr(
            module, "__tangram_module__", self.finder.tg.Module.from_data(self.data)
        )
        self.finder.sources[filename] = self.data
        linecache.cache[filename] = (
            len(self.text),
            None,
            self.text.splitlines(True),
            filename,
        )
        if self.package:
            self.finder.packages[module.__name__] = self.data
            module.__path__ = [str(Path(filename).parent)]
        try:
            exec(compile(self.text, filename, "exec"), module.__dict__)
        except BaseException:
            self.finder.modules.pop(self.identity, None)
            self.finder.packages.pop(module.__name__, None)
            raise

    def get_source(self, fullname: str) -> str:
        return self.text

    def get_filename(self, fullname: str) -> str:
        return self.filename


class Finder(importlib.abc.MetaPathFinder):
    def __init__(self, host: Host, tg: ModuleType, entry: dict[str, Any]) -> None:
        self.host = host
        self.tg = tg
        self.entry = entry
        self.inventory = json.loads(host.inventory(json.dumps(entry)))
        self.entry_path = (
            self.inventory["entry"] if self.inventory else entry["referent"]["node"]
        )
        self.paths: dict[str, str] = {}
        self.packages: dict[str, dict[str, Any]] = {}
        self.modules: dict[str, ModuleType] = {}
        self.sources: dict[str, dict[str, Any]] = {}
        self.root = "_tangram_entry"

    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None = None,
        target: ModuleType | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        parent, _, name = fullname.rpartition(".")
        referrer = self.packages.get(parent)
        if referrer is None:
            return None
        directory = Path(self.paths[parent]).parent
        file = directory / f"{name}.tg.py"
        package = directory / name / "tangram.py"
        files = self.inventory["modules"] if self.inventory else None
        is_file = str(file) in files if files is not None else file.is_file()
        is_package = str(package) in files if files is not None else package.is_file()
        if is_file and is_package:
            raise ImportError(f"ambiguous Tangram module: {file} and {package}")
        if is_file:
            return self.spec(fullname, referrer, f"./{name}.tg.py", False, str(file))
        if is_package:
            return self.spec(
                fullname, referrer, f"./{name}/tangram.py", True, str(package)
            )
        namespace = (
            any(path.startswith(str(directory / name) + "/") for path in files)
            if files is not None
            else (directory / name).is_dir()
        )
        if namespace:
            # Namespace packages anchor relative resolution without executing code.
            data = json.loads(json.dumps(referrer))
            data["referent"]["node"] = str(directory / name / "tangram.py")
            self.packages[fullname] = data
            self.paths[fullname] = str(directory / name / "tangram.py")
            spec = importlib.machinery.ModuleSpec(fullname, None, is_package=True)
            spec.submodule_search_locations = [str(directory / name)]
            return spec
        raise ModuleNotFoundError(
            f"no Tangram module named {fullname!r}", name=fullname
        )

    def spec(
        self,
        fullname: str,
        referrer: dict[str, Any],
        reference: str | None,
        package: bool,
        filename: str,
    ) -> importlib.machinery.ModuleSpec:
        if self.inventory:
            data, text = self.inventory["modules"][posixpath.normpath(filename)]
        else:
            serialized, text = self.host.module(json.dumps(referrer), reference)
            data = json.loads(serialized)
            filename = data["referent"]["node"]
        self.paths[fullname] = filename
        loader = Loader(self, data, text, package, filename)
        spec = importlib.util.spec_from_loader(fullname, loader, is_package=package)
        assert spec is not None
        spec.origin = filename
        spec.has_location = True
        return spec

    def load_entry(self) -> ModuleType:
        path = Path(self.entry_path)
        package = path.name == "tangram.py"
        if package:
            fullname = self.root
        else:
            name = path.name.removesuffix(".tg.py")
            if not name.isidentifier():
                raise ImportError(
                    "the entry module basename must be a Python identifier"
                )
            fullname = f"{self.root}.{name}"
            root = ModuleType(self.root)
            root.__package__ = self.root
            root.__path__ = [str(path.parent)]
            root.__spec__ = importlib.machinery.ModuleSpec(
                self.root, None, is_package=True
            )
            root.__spec__.submodule_search_locations = root.__path__
            sys.modules[self.root] = root
            self.packages[self.root] = self.entry
            self.paths[self.root] = str(path)
        spec = self.spec(fullname, self.entry, None, package, str(path))
        module = importlib.util.module_from_spec(spec)
        sys.modules[fullname] = module
        try:
            assert isinstance(spec.loader, Loader)
            spec.loader.exec_module(module)
        except BaseException:
            sys.modules.pop(fullname, None)
            raise
        if not package:
            setattr(sys.modules[self.root], fullname.rsplit(".", 1)[1], module)
        return module

    def close(self) -> None:
        for name in list(sys.modules):
            if name == self.root or name.startswith(self.root + "."):
                del sys.modules[name]

    def error(
        self,
        exception: BaseException,
        seen: set[int] | None = None,
    ) -> dict[str, Any]:
        seen = set() if seen is None else seen
        seen.add(id(exception))
        locations = []
        for frame in traceback.extract_tb(exception.__traceback__):
            data = self.sources.get(frame.filename)
            if data is None:
                continue
            start = self.position(frame.filename, frame.lineno or 1, frame.colno or 0)
            end = self.position(
                frame.filename,
                frame.end_lineno or frame.lineno or 1,
                frame.end_colno or 0,
            )
            locations.append(
                {
                    "file": {"kind": "module", "value": data},
                    "range": {"start": start, "end": end},
                    "symbol": frame.name,
                }
            )
        if isinstance(exception, SyntaxError) and exception.filename in self.sources:
            position = self.position(
                exception.filename,
                exception.lineno or 1,
                max((exception.offset or 1) - 1, 0),
                utf8=False,
            )
            locations.append(
                {
                    "file": {
                        "kind": "module",
                        "value": self.sources[exception.filename],
                    },
                    "range": {"start": position, "end": position},
                }
            )
        error = {"message": f"{type(exception).__name__}: {exception}"}
        if isinstance(exception, self.tg.Error):
            try:
                error = exception.to_data()
            except ValueError:
                pass
        if locations:
            error.setdefault("location", locations[-1])
            error.setdefault("stack", list(reversed(locations)))
        source = exception.__cause__ or (
            exception.__context__ if not exception.__suppress_context__ else None
        )
        if source is not None and id(source) not in seen and "source" not in error:
            error["source"] = {"node": self.error(source, seen)}
        return error

    def position(
        self,
        filename: str,
        line: int,
        column: int,
        *,
        utf8: bool = True,
    ) -> dict[str, int]:
        lines = linecache.getlines(filename)
        text = lines[line - 1] if 0 < line <= len(lines) else ""
        # CPython traceback columns are UTF-8 byte offsets; Tangram uses UTF-16.
        prefix = (
            text.encode("utf-8")[:column].decode("utf-8") if utf8 else text[:column]
        )
        return {"line": line - 1, "character": len(prefix.encode("utf-16-le")) // 2}


async def execute(
    context: dict[str, Any],
    finder: Finder,
    tg: ModuleType,
) -> tuple[int, str | None, str | None]:
    client = tg.client
    client.url = context["url"]
    client.token = context["token"]
    try:
        namespace = finder.load_entry()
        name = context["export"]
        if name is None:
            return 0, None, None
        if name not in vars(namespace):
            raise ValueError(f"failed to find the export named {name}")
        value = getattr(namespace, name)
        while inspect.isawaitable(value):
            value = await value
        if callable(value):
            value = value(*tg.process.args)
        value = await tg.resolve(value)
        if not tg.Value.is_(value):
            raise TypeError("the export must be a Tangram value or a function")
        await tg.Value.store(value, client)
        return 0, json.dumps(tg.Value.to_data(value)), None
    except SystemExit as exception:
        code = exception.code
        if code is None:
            return 0, None, None
        if isinstance(code, int):
            return code % 256, None, None
        print(code, file=sys.stderr)
        return 1, None, None
    except BaseException as exception:
        traceback.print_exception(exception)
        return 1, None, json.dumps(finder.error(exception))
    finally:
        await client.close()
        sys.stdout.flush()
        sys.stderr.flush()


def run(context: str, sources: str, host: Host) -> tuple[int, str | None, str | None]:
    context = json.loads(context)
    embedded = Embedded(json.loads(sources))
    sys.meta_path.insert(0, embedded)
    finder = None
    try:
        tg = importlib.import_module("tangram")
        tg.process.set_process(
            {
                "args": [tg.Value.from_data(value) for value in context["args"]],
                "cwd": context["cwd"],
                "env": {
                    key: tg.Value.from_data(value)
                    for key, value in context["env"].items()
                },
                "export": context["export"],
                "module": tg.Module.from_data(context["module"]),
            }
        )
        finder = Finder(host, tg, context["module"])
        sys.meta_path.insert(0, finder)
        with asyncio.Runner() as runner:
            return runner.run(execute(context, finder, tg))
    finally:
        if finder is not None:
            finder.close()
            sys.meta_path.remove(finder)
        sys.meta_path.remove(embedded)
        sys.stdout.flush()
        sys.stderr.flush()
