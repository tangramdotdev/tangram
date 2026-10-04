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

`tg format` formats `tangram.py` and `.tg.py` modules using Ruff, with four-space
indentation and an 88-column line width. It accepts a file or directory and respects
`.tangramignore`. The LSP document-formatting request uses the same formatter.

`tg check ./main.tg.py` type-checks Python modules with ty. It checks imported
modules once per canonical referent, resolves declared path and tag dependencies
through Tangram, and reads checked-in source without executing Python. Diagnostics
refer to the original modules and source positions, retaining explanatory notes.
Unresolved imports produce diagnostics without suppressing other type errors.
Standard-library imports use ty's bundled typeshed. The checker embeds the existing
Python client's annotated source and its dependencies; the ambient `tg` name refers
to that same package. No separate client declarations are maintained.

The runtime and checker use the same Rust resolver for explicit packages
(`tangram.py`), namespace packages, relative imports, and PEP 723 dependency aliases
and attributes. Package ancestry, relative-import bounds, and recorded dependency
edges are interpreted once, independently of execution or type inference.
The shared resolver also decides whether a from-import keeps an existing export or
resolves a child module. A failed recorded dependency remains an error even when
the package initializer exports that name. Declared aliases take precedence over
ordinary library imports, including names such as `sys`.
Object imports use the runtime's synthetic module generator and expose typed
`default` exports. Python LSP features remain outside this integration. Building it currently requires
the modified ty checkout at `../ty/ruff`; Cargo uses local path dependencies for
the resolver and checker crates. Ruff parsing and formatting retain their existing
pinned revision.

Relative imports require a package context and use Python syntax:

```python
from .helper import value
from . import sub
from ..other import function
```

`helper` resolves to `helper.tg.py`; `sub` resolves to `sub/tangram.py`. A directory
without `tangram.py`, imported within an existing package, is a namespace package.
Standalone modules have an empty `__package__` and reject relative imports.
An explicit `tangram.py` establishes a package. Package ancestry comes from the
resolved `tg.Module` source path or its referent's `id` and `path`, independently
of the import alias or order. Namespace ancestry in checked-in modules requires
retained path information. Directory members retain the containing artifact in
`id` and their member path in `path`; file check-ins retain the source path and
recorded initializer dependencies. Bare files do not inherit package context
from their importers. `..` follows
package parents and cannot cross the top-level package or referent artifact root. Having both
`helper.tg.py` and `helper/tangram.py` is an error. Plain `.py` siblings are not Tangram
modules. Standard-library and client imports use their normal names. Relative
resolution uses the importing package's directory, independent of the current
working directory. Module descriptors are available as `__tangram_module__`.
The runtime uses the same resolve, cache, and load sequence as JS: the shared Rust
resolver produces a descriptor, its token-free identity selects a cached module,
and the shared module API loads source only when execution needs it. Cached
descriptors receive refreshed authorization tokens. A recorded dependency is
authoritative, including its referent options and unresolved state; directory
member lookup applies only when no dependency was recorded. Entry graph pointers
are prepared in the runtime for every invocation route. Modules register before
execution to support cycles; failed initializations can be retried, and cached
children look up their current parent by its canonical cache name. Resolver and
loader failures preserve structured Tangram errors, as in JS.
Child entries and declared child imports initialize an adjacent `tangram.py` before
executing the child, for both filesystem and checked-in modules. Check-in records
explicit ancestor package initializers as dependencies, including across namespace directories, so their
exports remain available without the original source directory. Package exports
take precedence over sibling module discovery. `find_spec` resolves the target
without executing it; parent packages initialize when needed.

Declare dependencies in a [PEP 723](https://peps.python.org/pep-0723/) script block:

```python
# /// script
# requires-python = ">=3.14,<3.15"
# [tool.tangram.imports.debug]
# specifier = "tools/^1"
# attributes = { get = "debug/tangram.py" }
# [tool.tangram.imports.release]
# specifier = "tools/^1"
# attributes = { get = "release/tangram.py" }
# ///

import debug
from release import value
```

Each name belongs to the declaring module. Declarations use the same specifier and
attribute conversion as JavaScript imports; identical specifiers with different
attributes can resolve to different dependencies. Declared names also work with
`__import__`, `importlib.import_module`, and `importlib.util.find_spec`. Package
submodules and namespace packages load from the resolved directory artifact.
Object imports, such as `attributes = { type = "file" }`, expose the Tangram object
as `default`. As in the JS loader, filesystem object imports expose `None` until
checked in. Python cannot execute JS, TS, or declaration modules.

Checkin marks `.tg.py` and `tangram.py` files as Python modules. Astral's Ruff parser
finds relative `from` imports and records their existing module files and package
initializers as dependencies. The shared metadata parser records every declared
import, including declarations used only by conditional or computed imports.
The runtime follows dependencies per referring module, so colliding relative paths
remain distinct and checked-in execution does not require the original source
files. Computed relative imports require a recorded edge or a member of the resolved
directory artifact. Undeclared absolute imports use the embedded libraries.

Invalid metadata produces source locations. `requires-python` must accept the pinned
embedded interpreter. Nonempty PEP 508 `dependencies` are rejected; Tangram imports
belong in `tool.tangram.imports`. Unclosed blocks are ignored and duplicate completed
blocks of the same type are rejected according to PEP 723.

Each invocation owns an asyncio runner, closes the default client connection, and
cancels pending tasks when the runner closes. Invocations in one process are
serialized because Python modules and the client process context are shared.
Exported functions can create commands with `tg.command(function, *args)` or
`await tg.Command.py(function, args)`. The latter returns an authorization-bearing
command referent, like JS `Command.js`. `tg.host.magic(function)` identifies the
function by its defining module and an export binding with the same object identity;
renamed exports and decorators exposing `__wrapped__` are supported. Functions without a Tangram module or an export
binding are rejected. Command and process builders encode fluent arguments with
`-a` for explicit string arguments and `-A` for Tangram values. Shared awaitables
resolve once, including those shared between initial arguments and builder setters.

Filesystem modules are checked in before constructing a function command, so its
dependencies and authorization survive execution in another process. Python
commands retain the module's directory ID and path because package member
loading needs them; the command referent retains the full original options. Callable
return types propagate to command and process results. Python cannot currently map
each callable parameter to its corresponding unresolved argument type, so callable
arguments are checked as Tangram values rather than against the parameter tuple.

Run integration tests with:

```sh
nu packages/cli/test.nu --no-cloud 'py/'
```
