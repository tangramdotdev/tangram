"""Load the embedded Python library without reading installation files."""

import _frozen_importlib as bootstrap
import _frozen_importlib_external as external
import _imp
import sys

PREFIX = "/tangram/python/"


class Finder:
    def __init__(self, assets):
        self.assets = assets
        self.frozen = {
            name.replace(".", "/")
            + ("/__init__.py" if _imp.is_frozen_package(name) else ".py"): name
            for name in getattr(_imp, "_frozen_module_names")()
        }
        self.module_paths = assets.paths | self.frozen.keys()
        self.directories = {""}
        for filename in self.module_paths:
            directory = filename.rpartition("/")[0]
            while directory:
                self.directories.add(directory)
                directory = directory.rpartition("/")[0]

    def find_spec(self, fullname, path=None, target=None):
        paths = [PREFIX.rstrip("/")] if path is None else path
        spec = self.search(fullname, paths)
        if spec is not None and spec.loader is None:
            paths = getattr(external, "_NamespacePath")(
                fullname, spec.submodule_search_locations, self.search
            )
            spec.loader = NamespaceLoader(self.assets, paths)
            spec.submodule_search_locations = paths
        return spec

    def search(self, fullname, paths):
        name = fullname.rpartition(".")[2]
        namespaces = []
        for path in paths:
            try:
                directory = directory_for_path(path)
            except ImportError:
                continue
            relative = directory + "/" + name if directory else name
            for filename, package in [
                (relative + "/__init__.py", True),
                (relative + ".py", False),
            ]:
                if filename in self.module_paths:
                    return self.spec(fullname, filename, package)
            if relative in self.directories:
                namespaces.append(PREFIX + relative)
        if not namespaces:
            return None
        spec = bootstrap.ModuleSpec(fullname, None, is_package=True)
        spec.submodule_search_locations = namespaces
        return spec

    def spec(self, fullname, filename, package):
        frozen = self.frozen.get(filename)
        loader = Loader(self.assets, filename, package, frozen)
        # The importlib ABCs are unavailable until this loader is installed.
        spec = bootstrap.ModuleSpec(
            fullname,
            loader,  # ty: ignore[invalid-argument-type]
            origin="frozen" if frozen is not None else PREFIX + filename,
            is_package=package,
        )
        spec.has_location = frozen is None
        if package:
            spec.submodule_search_locations = [PREFIX + filename.rpartition("/")[0]]
        return spec

    def path_hook(self, path):
        directory_for_path(path)
        return PathFinder(self, path)

    def iter_modules(self, prefix=""):
        return PathFinder(self, PREFIX.rstrip("/")).iter_modules(prefix)

    def find_distributions(self, context=None):
        import importlib.metadata
        import re

        if context is not None and "path" in vars(context):
            for path in context.path:
                try:
                    directory = directory_for_path(path)
                except ImportError:
                    continue
                if directory == "":
                    break
            else:
                return
        name = context.name if context is not None else None

        def normalize(name):
            return re.sub(r"[-_.]+", "_", name).lower()

        directories = sorted({path.split("/", 1)[0] for path in self.assets.paths})
        for directory in directories:
            if not directory.endswith(".dist-info"):
                continue
            distribution = importlib.metadata.PathDistribution(
                Resource(self.assets, directory)
            )
            if name is None or normalize(distribution.metadata["Name"]) == normalize(
                name
            ):
                yield distribution


class PathFinder:
    def __init__(self, finder, path):
        self.finder = finder
        self.path = path
        self.directory = directory_for_path(path)

    def find_spec(self, fullname, target=None):
        return self.finder.search(fullname, [self.path])

    def iter_modules(self, prefix=""):
        directory = self.directory + "/" if self.directory else ""
        modules = {}
        for path in sorted(self.finder.module_paths):
            if not path.startswith(directory):
                continue
            relative = path[len(directory) :]
            name, separator, child = relative.partition("/")
            if separator:
                if child == "__init__.py" and name.isidentifier():
                    modules[name] = True
            elif name.endswith(".py"):
                name = name[:-3]
                if name != "__init__" and name.isidentifier():
                    modules.setdefault(name, False)
        return ((prefix + name, package) for name, package in sorted(modules.items()))


class Loader(external.SourceLoader):
    def __init__(self, assets, filename, package, frozen=None):
        self.assets = assets
        self.frozen = frozen
        self.filename = filename
        self.package = package

    def get_code(self, fullname):
        if self.frozen is not None:
            return bootstrap.FrozenImporter.get_code(self.frozen)
        return compile(
            self.get_data(PREFIX + self.filename),
            self.get_filename(fullname),
            "exec",
            dont_inherit=True,
        )

    def get_source(self, fullname):
        if self.frozen is not None:
            return None
        return external.decode_source(self.get_data(PREFIX + self.filename))

    def get_filename(self, fullname):
        return PREFIX + self.filename

    def is_package(self, fullname):
        return self.package

    def get_data(self, path):
        if not path.startswith(PREFIX):
            raise FileNotFoundError(path)
        value = self.assets.get(path[len(PREFIX) :])
        if value is None:
            raise FileNotFoundError(path)
        return value

    def get_resource_reader(self, fullname):
        return Reader(self.assets, self.filename.rpartition("/")[0])


class NamespaceLoader:
    def __init__(self, assets, paths):
        self.assets = assets
        self.paths = paths

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        module.__file__ = None

    def get_resource_reader(self, fullname):
        return Reader(self.assets, *(directory_for_path(path) for path in self.paths))


class Reader:
    def __init__(self, assets, directory, *directories):
        self.root = Resource(assets, directory)
        if directories:
            from importlib.resources.readers import MultiplexedPath

            self.root = MultiplexedPath(
                self.root, *(Resource(assets, path) for path in directories)
            )

    def files(self):
        return self.root

    def open_resource(self, resource):
        return self.root.joinpath(resource).open("rb")

    def resource_path(self, resource):
        raise FileNotFoundError("the resource is embedded in the executable")

    def is_resource(self, name):
        return self.root.joinpath(name).is_file()

    def contents(self):
        return (entry.name for entry in self.root.iterdir())


class Resource:
    def __init__(self, assets, path):
        self.assets = assets
        self.path = path

    def __str__(self):
        return PREFIX + self.path

    @property
    def parent(self):
        return Resource(self.assets, self.path.rpartition("/")[0])

    @property
    def name(self):
        return self.path.rsplit("/", 1)[-1]

    def exists(self):
        return self.is_file() or self.is_dir()

    def is_file(self):
        return self.path in self.assets.paths

    def is_dir(self):
        prefix = self.path + "/" if self.path else ""
        return any(path.startswith(prefix) for path in self.assets.paths)

    def iterdir(self):
        if not self.is_dir():
            raise NotADirectoryError(self.path)
        prefix = self.path + "/" if self.path else ""
        names = sorted(
            {
                path[len(prefix) :].split("/", 1)[0]
                for path in self.assets.paths
                if path.startswith(prefix)
            }
        )
        return (self.joinpath(name) for name in names)

    def joinpath(self, *descendants):
        path = self.path
        for descendant in descendants:
            descendant = str(descendant)
            if descendant.startswith("/"):
                raise ValueError("invalid embedded resource path")
            for part in descendant.split("/"):
                if part == "..":
                    raise ValueError("invalid embedded resource path")
                if part not in {"", "."}:
                    path = path + "/" + part if path else part
        return Resource(self.assets, path)

    def __truediv__(self, child):
        return self.joinpath(child)

    def open(self, mode="r", *args, **kwargs):
        import io

        if mode not in {"r", "rt", "rb"}:
            raise ValueError("embedded resources are read-only")
        if mode == "rb" and (args or kwargs):
            raise ValueError("binary mode does not accept text arguments")
        value = self.assets.get(self.path)
        if value is None:
            if self.is_dir():
                raise IsADirectoryError(self.path)
            raise FileNotFoundError(self.path)
        stream = io.BytesIO(value)
        if mode == "rb":
            return stream
        return io.TextIOWrapper(stream, *args, **kwargs)

    def read_bytes(self):
        with self.open("rb") as stream:
            return stream.read()

    def read_text(self, encoding=None, errors=None):
        with self.open(encoding=encoding, errors=errors) as stream:
            return stream.read()


def install(assets):
    assets.paths = frozenset(assets.names())
    finder = Finder(assets)
    # Attach embedded paths and resources to packages loaded during initialization.
    for module in list(sys.modules.values()):
        spec = getattr(module, "__spec__", None)
        if (
            spec is not None
            and spec.submodule_search_locations is not None
            and _imp.is_frozen(spec.name)
        ):
            filename = spec.name.replace(".", "/") + "/__init__.py"
            replacement = finder.spec(spec.name, filename, True)
            spec.loader = replacement.loader
            spec.submodule_search_locations = replacement.submodule_search_locations
            module.__loader__ = spec.loader
            module.__path__ = spec.submodule_search_locations
    # Keep built-in imports and load frozen and source modules through the finder.
    sys.meta_path[:] = [bootstrap.BuiltinImporter, finder]
    sys.path[:] = []
    sys.path_hooks[:] = [finder.path_hook]
    sys.path_importer_cache.clear()


def directory_for_path(path):
    if not isinstance(path, str):
        raise ImportError("not an embedded library path")
    if path == PREFIX.rstrip("/"):
        return ""
    if not path.startswith(PREFIX):
        raise ImportError("not an embedded library path")
    parts = []
    for part in path[len(PREFIX) :].split("/"):
        if part == "..":
            if not parts:
                raise ImportError("the path escapes the embedded library")
            parts.pop()
        elif part not in {"", "."}:
            parts.append(part)
    return "/".join(parts)
