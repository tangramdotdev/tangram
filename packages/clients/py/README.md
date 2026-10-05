# Python client

The standalone client imports as `tangram` and requires Python 3.12 or later.
It uses `asyncio` and `hyper-h2` for duplex HTTP/2 over TCP, TLS, and Unix sockets.
Python's standard library does not implement HTTP/2. A PyO3 extension uses the
Rust client for object IDs, TGON serialization, and checksums.

Install uv and Python 3.12 or later, then build an editable development installation
from the repository root:

```sh
python3 packages/clients/py/build.py --profile dev
```

This uses the installed interpreter and creates the workspace environment at
`.venv`. The root `pyproject.toml` declares the workspace and development tools;
`uv.lock` locks dependencies for all members. The build synchronizes the locked
dependencies before compiling the native extension with the requested profile.
You can also use `uv sync --locked --all-packages` to build the workspace with the
backend's default release profile.
The build backend configures PyO3 for the installed interpreter independently of
the embedded runtime's pinned CPython version.
Alternatively, install into an existing environment with
`python3 -m pip install ./packages/clients/py`.

The Python package is named `tangram`. Run a script or open a REPL in its workspace
environment with `uv run --package tangram python script.py` or
`uv run --package tangram python`.

```python
import asyncio
import tangram as tg


async def main():
    async with tg.Client() as client:
        directory = await tg.directory({"hello.txt": tg.file("Hello, World!")})
        await directory.store(client)
        loaded = tg.Directory.with_referent(directory.to_referent())
        file = await loaded.get("hello.txt", client)
        print(await file.text(client))


asyncio.run(main())
```

Builders are callable and awaitable. They resolve nested futures and other
builders, and expose fluent setters corresponding to JavaScript:

```python
command = tg.command().executable("sh").arg("-c", "echo hello").env({"HELLO": "world"})
output = await command.build()
```

On Python 3.14 or later, builders also accept native t-strings:

```python
template = await tg.template(t"cat {input_file} > {tg.output}")
file = await tg.file(t"""
    Hello, {name}!
""")
```

Template interpolations accept strings, artifacts, placeholders, other templates,
and futures of those values. File interpolations must resolve to strings, matching
JavaScript's tagged file constructor. Both builders remove indentation like their
JavaScript counterparts and preserve futures across repeated awaits. Use
`tg.Template.raw(t"...")` or `tg.File.Builder(True, t"...")` to preserve indentation.
Conversions (`!s`, `!r`, `!a`, including implicit `!r` in debug expressions) and
format specifications are rejected rather than stringifying artifact references.
Ordinary f-strings interpolate immediately and cannot preserve those references.
Python's native template type does not describe its interpolation types, so these
constraints are checked at runtime.

The client reads `TANGRAM_URL` and `TANGRAM_TOKEN`. `Client` also accepts explicit
URL and token arguments. Use its async context manager or close it explicitly.
Connections belong to the event loop that created them. Public Python names use
`snake_case`; wire fields follow the Rust API. `Object.store` returns an ID,
`Value.store` stores a value's objects, and `Client.write(bytes_or_string)` returns
a blob ID. `Client.write(arg, stream)` returns an output containing a referent.

The source layout follows `packages/clients/js/src`. A source file that also has
a directory becomes a Python package: `file.ts` corresponds to `file/__init__.py`,
and `file/xattrs.ts` corresponds to `file/xattrs.py`. The deliberate naming
exceptions are `index.ts` → `__init__.py`, `assert.ts` → `assert_.py` because
`assert` is reserved, and `host/node.ts` → `host/default.py` for the Python host.
A structural test checks every source counterpart and the defining modules of
public classes and builders. Per-file reviews cover the public APIs, wire codecs,
and implementation ownership.

The client covers artifacts, values, graph pointers, authorization, HTTP endpoint
operations, process connections and stdio, local command execution, and builtins.
Process connections resume reads and replay writes using their original positions.
`tg py` embeds the client and supports filesystem modules with relative `.tg.py`
imports; see [the runtime](../../py/README.md). Python function commands, import
attributes and compiler support are separate work.

Run checks and unit tests after building:

```sh
bun run check
bun run --filter @tangramdotdev/python-client test
```

Run server integration tests:

```sh
nu packages/cli/test.nu --no-cloud py/
```

The harness builds the client when selecting Python tests and assumes Python is
installed. Set `TANGRAM_PYTHON` to choose an interpreter. Integration tests cover
native serialization, xattrs, artifact storage, concurrent HTTP/2 streams, large
blob and process transfers, reconnecting process control, local execution, and
builtins.
