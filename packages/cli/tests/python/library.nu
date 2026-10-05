use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'my-script.v1.tg.py': '
        import _imp
        import asyncio
        import bz2
        import ctypes
        import email
        import encodings
        import importlib
        import hashlib
        import importlib.metadata
        import importlib.resources
        import inspect
        import json
        import json.encoder
        import lzma
        import os
        import pkgutil
        import sqlite3
        import ssl
        import sys
        from types import ModuleType
        import zlib

        def annotated(value: int) -> str:
            return str(value)

        async def default():
            assert annotated.__annotations__ == {"value": int, "return": str}
            assert list(importlib.metadata.distributions(path=[])) == []
            assert list(importlib.metadata.distributions(path=["/missing/library"])) == []
            distributions = {
                entry.metadata["Name"] for entry in importlib.metadata.distributions()
            }
            for path in ["/tangram/python/.", "/tangram/python/encodings/.."]:
                assert {
                    entry.metadata["Name"]
                    for entry in importlib.metadata.distributions(path=[path])
                } == distributions
            assert importlib.resources.files(json.encoder).joinpath("decoder.py").read_bytes()
            assert importlib.metadata.version("h2")
            assert importlib.metadata.version("PyYAML")
            assert importlib.metadata.version("tangram") == "0.0.0"
            assert importlib.metadata.packages_distributions()["tangram"] == ["tangram"]
            assert "tangram/__init__.py" in {
                str(path) for path in importlib.metadata.files("tangram")
            }
            assert "email.message" in {
                module.name for module in pkgutil.iter_modules(email.__path__, "email.")
            }
            assert "email.mime.text" in {
                module.name for module in pkgutil.walk_packages(email.__path__, "email.")
            }
            assert sys.path == []
            assert sys.prefix == "/tangram/python"
            assert sys.flags.isolated
            assert _imp.is_frozen("encodings")
            assert encodings.__path__ == ["/tangram/python/encodings"]
            assert encodings.__spec__.origin == "frozen"
            assert encodings.__spec__.has_location is False
            codecs = {
                module.name
                for module in pkgutil.iter_modules(encodings.__path__, "encodings.")
            }
            assert {"encodings.ascii", "encodings.cp1252", "encodings.utf_8"} <= codecs
            assert importlib.resources.files(encodings).joinpath("cp1252.py").read_bytes()
            assert importlib.reload(encodings) is encodings
            assert _imp.is_frozen("encodings")
            assert encodings.__path__ == ["/tangram/python/encodings"]
            assert importlib.resources.files(encodings).joinpath("cp1252.py").read_bytes()
            ascii_codec = importlib.import_module("encodings.ascii")
            assert ascii_codec.__spec__.origin == "frozen"
            assert _imp.is_frozen("encodings.ascii")
            saved_path = encodings.__path__
            del sys.modules["encodings.ascii"]
            for restricted_path in [[], ["/missing/encodings"]]:
                encodings.__path__ = restricted_path
                try:
                    importlib.import_module("encodings.ascii")
                except ModuleNotFoundError:
                    pass
                else:
                    raise AssertionError("the frozen importer ignored the package path")
            encodings.__path__ = saved_path
            assert importlib.import_module("encodings.ascii").__spec__.origin == "frozen"
            alias = ModuleType("_tangram_codec_alias")
            alias.__path__ = encodings.__path__
            sys.modules[alias.__name__] = alias
            try:
                redirected = importlib.import_module(alias.__name__ + ".ascii")
                assert redirected.__spec__.origin == "frozen"
                assert redirected.getregentry().name == "ascii"
                assert redirected.__package__ == alias.__name__
            finally:
                del sys.modules[alias.__name__ + ".ascii"]
                del sys.modules[alias.__name__]
            assert not _imp.is_frozen("os")
            assert os.__file__.startswith("/tangram/python/")
            assert ssl._ssl.__spec__.origin == "built-in"
            assert zlib.__spec__.origin == "built-in"
            for codec in [bz2, lzma, zlib]:
                assert codec.decompress(codec.compress(b"hello")) == b"hello"
            assert len(hashlib.sha256(b"hello").digest()) == 32
            with sqlite3.connect(":memory:") as database:
                assert database.execute("select 42").fetchone() == (42,)
            assert "def dumps(" in inspect.getsource(json.dumps)
            assert importlib.resources.files(email).joinpath("__init__.py").read_bytes()
            assert await asyncio.sleep(0, result=42) == 42
            print("embedded Python")
    '
}

# Interpreter initialization ignores external Python installations and search paths.
let output = with-env {PYTHONHOME: '/missing/python', PYTHONPATH: '/missing/modules'} {
    tg python --export default ($path | path join my-script.v1.tg.py) | complete
}
success $output
assert equal ($output.stdout | str trim) 'embedded Python'

# The same executable supplies the interpreter and libraries inside the sandbox.
let output = tg run ($path | path join my-script.v1.tg.py) | complete
success $output
assert equal ($output.stdout | str trim) 'embedded Python'
