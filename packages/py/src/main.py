"""Embedded client bootstrap and filesystem Tangram module imports."""

from __future__ import annotations

import asyncio
import builtins
import hashlib
import importlib.abc
import importlib.machinery
import importlib.util
import inspect
import json
import linecache
import sys
import traceback
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import ModuleType
from typing import Any, Protocol


class Host(Protocol):
    def describe(self, module: str) -> str: ...

    def resolve(self, request: str) -> str: ...

    def load(self, module: str) -> str: ...

    def metadata(self, filename: str, text: str) -> str: ...


class Module:
    def __init__(self, resolved: dict[str, Any]) -> None:
        self.data = resolved["data"]
        self.key = resolved["key"]
        self.filename = resolved["filename"]
        self.name = "_tangram_modules.m" + hashlib.sha256(self.key.encode()).hexdigest()
        self.package = resolved["package"]
        self.target = {"kind": "module", "value": resolved}
        self.namespace: ModuleType | None = None
        self.spec: importlib.machinery.ModuleSpec | None = None
        self.text = ""
        self.imports: dict[str, Any] = {}
        self.status = "new"
        self.bindings: dict[tuple[str, str], ModuleType] = {}


class Package:
    def __init__(self, namespace: ModuleType, target: dict[str, Any]) -> None:
        self.namespace = namespace
        self.target = target


class Loader(importlib.abc.Loader):
    def __init__(self, finder: Finder, module: Module) -> None:
        self.finder = finder
        self.module = module

    def create_module(self, spec: importlib.machinery.ModuleSpec) -> ModuleType:
        if self.module.namespace is None:
            self.module.namespace = ModuleType(spec.name)
        return self.module.namespace

    def exec_module(self, module: ModuleType) -> None:
        if module is not self.module.namespace:
            raise ImportError("the module does not match its loader")
        self.finder.namespace(self.module)

    def get_source(self, fullname: str) -> str:
        if self.module.status == "new":
            return self.finder.host.load(json.dumps(self.module.data))
        return self.module.text

    def get_filename(self, fullname: str) -> str:
        return self.finder.filename(self.module)


class Finder(importlib.abc.MetaPathFinder):
    def __init__(self, host: Host, tg: ModuleType, entry: dict[str, Any]) -> None:
        self.host = host
        self.tg = tg
        self.modules: dict[str, Module] = {}
        self.names: dict[str, Module] = {}
        self.packages: dict[str, Package] = {}
        self.sources: dict[str, dict[str, Any]] = {}
        self.linecache = {}
        self.original_import_module = importlib.import_module
        self.original_find_spec = importlib.util.find_spec
        self.entry = self.register(json.loads(host.describe(json.dumps(entry))))

    def register(self, resolved: dict[str, Any]) -> Module:
        # Match the JS cache identity and refresh the resolved authorization tokens.
        key = resolved["key"]
        module = self.modules.get(key)
        if module is None:
            module = Module(resolved)
            self.modules[key] = module
            self.names[module.name] = module
        else:
            module.data = resolved["data"]
            module.target = {"kind": "module", "value": resolved}
            filename = self.filename(module)
            if filename in self.sources:
                self.sources[filename] = module.data
            if module.namespace is not None:
                module.namespace.__dict__["__tangram_module__"] = (
                    self.tg.Module.from_data(module.data)
                )
        return module

    def resolve(self, request: dict[str, Any]) -> dict[str, Any] | None:
        output = json.loads(self.host.resolve(json.dumps(request)))
        if output["kind"] == "fallback":
            return None
        if output["kind"] == "missing":
            error = output["value"]
            if error["name"] is not None:
                raise ModuleNotFoundError(error["message"], name=error["name"])
            raise ImportError(error["message"])
        return output["value"]

    def target_object(self, target: dict[str, Any]) -> Module | Package:
        if target["kind"] == "module":
            return self.register(target["value"])
        value = target["value"]
        name = "_tangram_modules.p" + hashlib.sha256(value["key"].encode()).hexdigest()
        if name not in self.packages:
            self.namespace(self.target_object(value["parent"]))
            # Loading the parent may already have created this namespace.
            if name not in self.packages:
                spec = importlib.machinery.ModuleSpec(name, None, is_package=True)
                spec.submodule_search_locations = []
                namespace = importlib.util.module_from_spec(spec)
                sys.modules[name] = namespace
                self.packages[name] = Package(namespace, target)
        package = self.packages[name]
        # Keep directory lookup when file dependencies reuse the namespace.
        if (
            package.target["value"]["referrer"]["kind"] != "directory"
            or value["referrer"]["kind"] == "directory"
        ):
            package.target = target
        return package

    def resolution(
        self, resolved: dict[str, Any]
    ) -> tuple[Module | Package, ModuleType | None]:
        steps = resolved["steps"]
        for index, step in enumerate(steps):
            parent = self.namespace(self.target_object(step["parent"]))
            child = self.target_object(step["target"])
            if isinstance(child, Module):
                child.bindings[(parent.__name__, step["name"])] = parent
            if isinstance(child, Package) or index < len(steps) - 1:
                setattr(parent, step["name"], self.namespace(child))
        root = resolved["root"]
        return self.target_object(resolved["target"]), (
            self.namespace(self.target_object(root)) if root is not None else None
        )

    def filename(self, module: Module) -> str:
        node = module.data["referent"]["node"]
        if isinstance(node, str) and node.startswith((".", "/")):
            return module.filename
        key = hashlib.sha256(module.key.encode()).hexdigest()
        return f"/tangram/modules/{key}/{Path(module.filename).name}"

    def spec(self, module: Module) -> importlib.machinery.ModuleSpec:
        if module.spec is None:
            loader = Loader(self, module)
            spec = importlib.util.spec_from_loader(
                module.name, loader, is_package=module.package
            )
            assert spec is not None
            module.spec = spec
        return module.spec

    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None = None,
        target: ModuleType | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        module = self.names.get(fullname)
        return self.spec(module) if module is not None else None

    def load(self, module: Module) -> ModuleType:
        if module.status in ("executing", "loaded"):
            assert module.namespace is not None
            return module.namespace
        if module.status == "new":
            # Load source after resolving the module and checking the cache.
            module.text = self.host.load(json.dumps(module.data))
            filename = self.filename(module)
            self.sources[filename] = module.data
            if filename not in self.linecache:
                self.linecache[filename] = linecache.cache.get(filename)
            linecache.cache[filename] = (
                len(module.text),
                None,
                module.text.splitlines(True),
                filename,
            )
            metadata = json.loads(self.host.metadata(filename, module.text))
            if "error" in metadata:
                location = metadata["error"].get("location")
                if location is not None:
                    location["file"] = {"kind": "module", "value": module.data}
                error = self.tg.Error.from_data(metadata["error"])
                if location is not None:
                    start = location["range"]["start"]
                    error.add_note(
                        f"{filename}:{start['line'] + 1}:{start['character'] + 1}"
                    )
                raise error
            module.imports = metadata["imports"]
            namespace = importlib.util.module_from_spec(self.spec(module))
            module.namespace = namespace
            sys.modules[module.name] = namespace
            namespace.__dict__.update(
                tg=self.tg,
                __tangram_module__=self.tg.Module.from_data(module.data),
                __builtins__={**vars(builtins), "__import__": self.import_},
            )
            module.status = "prepared"
        namespace = module.namespace
        assert namespace is not None
        try:
            if module.data["kind"] == "py":
                if module.package:
                    self.packages.setdefault(
                        module.name, Package(namespace, module.target)
                    )
                    namespace.__package__ = module.name
                resolved = self.resolve(
                    {"kind": "package", "module": module.data, "parent": module.package}
                )
                parent = (
                    self.namespace(self.target_object(resolved["target"]))
                    if resolved is not None
                    else None
                )
                if not module.package:
                    namespace.__package__ = (
                        parent.__name__ if parent is not None else ""
                    )
            # A package initializer may have imported and executed this child already.
            if module.status in ("executing", "loaded"):
                return namespace
            module.status = "executing"
            exec(
                compile(module.text, self.filename(module), "exec", dont_inherit=True),
                namespace.__dict__,
            )
            module.status = "loaded"
        except BaseException:
            sys.modules.pop(module.name, None)
            self.packages.pop(module.name, None)
            module.namespace = None
            module.status = "new"
            raise
        return namespace

    def namespace(self, target: Module | Package) -> ModuleType:
        if isinstance(target, Package):
            return target.namespace
        namespace = self.load(target)
        for (_, name), parent in target.bindings.items():
            setattr(parent, name, namespace)
        return namespace

    def context(self, globals: Mapping[str, Any] | None) -> Module | None:
        return self.names.get(globals.get("__name__")) if globals is not None else None

    def target(
        self, name: str, globals: Mapping[str, Any] | None, level: int = 0
    ) -> dict[str, Any] | None:
        referrer = self.context(globals)
        if referrer is None:
            return None
        return self.resolve(
            {
                "kind": "import",
                "imports": referrer.imports,
                "level": level,
                "name": name,
                "referrer": referrer.data,
            }
        )

    def fromlist(
        self,
        namespace: ModuleType,
        context: dict[str, Any] | None,
        names: Sequence[str],
    ) -> ModuleType:
        if context is None:
            return namespace
        names = getattr(namespace, "__all__", ()) if "*" in names else names
        output = namespace
        for name in names:
            value = getattr(namespace, name, None)
            export = "absent"
            if hasattr(namespace, name):
                export = (
                    "module"
                    if isinstance(value, ModuleType) and value.__name__ in self.names
                    else "value"
                )
            try:
                resolved = self.resolve(
                    {
                        "kind": "member",
                        "context": context,
                        "export": export,
                        "name": name,
                    }
                )
            except ModuleNotFoundError:
                continue
            if resolved is None:
                continue
            # A from-import binds its child below, preserving referrer-specific exports.
            value = self.namespace(self.target_object(resolved["target"]))
            if hasattr(namespace, name) and getattr(namespace, name) is not value:
                # Keep this edge from changing another module's package exports.
                if output is namespace:
                    output = ModuleType(namespace.__name__)
                    output.__dict__.update(namespace.__dict__)
                setattr(output, name, value)
            else:
                setattr(namespace, name, value)
                if output is not namespace:
                    setattr(output, name, value)
        return output

    def import_(
        self,
        name: str,
        globals: Mapping[str, Any] | None = None,
        locals: Mapping[str, Any] | None = None,
        fromlist: Sequence[str] = (),
        level: int = 0,
    ) -> ModuleType:
        globals = sys._getframe(1).f_globals if globals is None else globals
        target = self.target(name, globals, level)
        if target is None:
            return builtins.__import__(name, globals, locals, fromlist, level)
        module, root = self.resolution(target)
        namespace = self.namespace(module)
        if fromlist:
            return self.fromlist(namespace, target["context"], fromlist)
        return root or namespace

    def dynamic_target(
        self, name: str, package: str | None, globals: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        if not name.startswith("."):
            return self.target(name, globals)
        if package not in self.packages:
            return None
        referrer = self.context(globals)
        level = len(name) - len(name.lstrip("."))
        if (
            referrer is not None
            and referrer.namespace is not None
            and package == referrer.namespace.__package__
        ):
            return self.target(name[level:], globals, level)
        return self.resolve(
            {
                "kind": "relative",
                "level": level,
                "name": name[level:],
                "package": self.packages[package].target,
            }
        )

    def import_module(self, name: str, package: str | None = None) -> ModuleType:
        target = self.dynamic_target(name, package, sys._getframe(1).f_globals)
        return (
            self.namespace(self.resolution(target)[0])
            if target is not None
            else self.original_import_module(name, package)
        )

    def find_module_spec(
        self, name: str, package: str | None = None
    ) -> importlib.machinery.ModuleSpec | None:
        target = self.dynamic_target(name, package, sys._getframe(1).f_globals)
        if target is None:
            return self.original_find_spec(name, package)
        module, _ = self.resolution(target)
        return (
            self.spec(module)
            if isinstance(module, Module)
            else module.namespace.__spec__
        )

    def load_entry(self) -> ModuleType:
        return self.load(self.entry)

    def close(self) -> None:
        for name in list(sys.modules):
            if name.startswith("_tangram_modules."):
                del sys.modules[name]
        setattr(importlib, "import_module", self.original_import_module)
        setattr(importlib.util, "find_spec", self.original_find_spec)
        for filename, entry in self.linecache.items():
            if entry is None:
                linecache.cache.pop(filename, None)
            else:
                linecache.cache[filename] = entry
        self.linecache.clear()

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
            # CPython converts the code to a signed C long, using -1 on overflow.
            if not -sys.maxsize - 1 <= code <= sys.maxsize:
                return 255, None, None
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


def run(context: str, host: Host) -> tuple[int, str | None, str | None]:
    context = json.loads(context)
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
        setattr(importlib, "import_module", finder.import_module)
        setattr(importlib.util, "find_spec", finder.find_module_spec)
        with asyncio.Runner() as runner:
            return runner.run(execute(context, finder, tg))
    finally:
        if finder is not None:
            finder.close()
            sys.meta_path.remove(finder)
        sys.stdout.flush()
        sys.stderr.flush()
