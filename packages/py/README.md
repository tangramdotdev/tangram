# Python runtime

`tg py` embeds CPython using PyO3. It runs a filesystem or checked-in entry module,
loads relative Tangram modules through the server's module resolver and loader,
and executes an optional named export. `tg run` recognizes Python modules and
invokes their `default` export through this runtime.

Every Tangram Python module receives `tg` in its globals, so `tg.file(...)` and the
rest of the client API work without an import. Standalone scripts executed by
Python directly use `import tangram as tg`.

Install uv and Python 3.12 or later to prepare the workspace dependencies, then build:

```sh
uv sync --locked --all-packages
cargo build --all-features --bin tangram
```

`build.rs` downloads checksum-pinned CPython 3.14.8 full distributions from
Astral's python-build-standalone release 20261003. It uses the pinned host
interpreter to freeze only the import and encoding bootstrap. The remaining
standard library, Tangram client, and locked pure Python dependencies are embedded
as compressed source with `rust-embed`, including in debug builds. The loader
decompresses and compiles modules in memory; it also supplies package resources
and source inspection. No Python installation or extracted support directory is
needed at runtime. Virtual library filenames start with `/tangram/python/`.

CPython and native standard-library modules are linked statically into `tg`.
macOS and Linux support aarch64 and x86_64. Linux musl supports both the default
static CRT and `-C target-feature=-crt-static`; the distribution variant follows
that setting. Optimized distributions contain LLVM bitcode, which the build
converts to native objects using the Rust toolchain's LLVM library. CPython's
mimalloc symbols are renamed to keep its allocator separate from Tangram's.

Fully static musl builds still need static archives for Tangram's existing sandbox
dependencies: set `LIBCAPNG_LINK_TYPE=static` and `LIBSECCOMP_LINK_TYPE=static`,
with `LIBCAPNG_LIB_PATH` and `LIBSECCOMP_LIB_PATH` when their archives are outside
Rust's library search paths. Dynamic musl builds use the normal shared sandbox
dependencies. Neither mode links a shared libpython.

The interpreter ignores `PYTHONHOME`, `PYTHONPATH`, site packages, and filesystem
extension loading. Built-in extensions include SSL, SQLite, ctypes, and compression
modules. Tk, curses, readline, and CPython test extensions are excluded. Python
source files and resources load through the embedded finder, while Tangram modules
use the referrer-aware resolver described below.

Build prerequisites include `curl`, `tar` with zstd support, a C compiler, and the
workspace's locked Python dependencies. Native musl objects also require
`llvm-objcopy` or GNU `objcopy`. For offline builds,
`TANGRAM_PYTHON_DISTRIBUTION` and `TANGRAM_PYTHON_HOST_DISTRIBUTION` can point to
the extracted target and host `python` directories containing `PYTHON.json`;
these overrides must match the pinned version, target, and ABI.

```sh
tg py ./main.tg.py
tg py --export default -a hello -A true ./tangram.py trailing
tg run ./main.tg.py
```

With no export, only module initialization runs. With an export, the runtime awaits
its outer awaitables, calls it with `tg.process.args` if callable, recursively
resolves the result, and stores its referenced objects. `None` becomes Tangram null.
When `TANGRAM_OUTPUT` is set, an export result or error is written with the same JSON
outcome and empty `user.tangram.outcome` xattr as `tg js`. Ordinary stdout is retained.
Exceptions produce exit status 1 and structured Tangram errors with source locations;
`SystemExit` retains Python's process exit semantics.

Relative imports use Python syntax:

```python
from .helper import value
from . import sub
from ..other import function
```

`helper` resolves to `helper.tg.py`; `sub` resolves to `sub/tangram.py`. A directory
without `tangram.py` is a namespace package for relative imports. Having both
`helper.tg.py` and `helper/tangram.py` is an error. Plain `.py` siblings are not Tangram
modules. Standard-library and client imports use their normal names. Relative
resolution uses the importing package's directory, independent of the current
working directory. Module descriptors are available as `__tangram_module__`.
Python's import machinery registers modules before executing them and supports
circular imports; the loader additionally caches by Tangram module identity, with
tokens excluded from identity. Failed initializations are removed from the cache.

Checkin marks `.tg.py` and `tangram.py` files as Python modules. Astral's Ruff parser and AST find
ordinary relative `from` imports and records their existing module files and package
initializers as dependencies. The runtime resolves and loads the checked-in module
graph through Tangram's module APIs, so execution does not require the original
source files. Absolute imports remain standard-library or embedded-client imports.
Computed imports do not add checkin dependencies; import attributes and structured
dependency comments are not supported yet.

Each invocation owns an asyncio runner, closes the default client connection, and
cancels pending tasks when the runner closes. Invocations in one process are
serialized because Python modules and the client process context are shared.
Python function commands remain separate work.

Run integration tests with:

```sh
nu packages/cli/test.nu --no-cloud 'py/(run|runtime|relative|resolution|errors|library)'
```
