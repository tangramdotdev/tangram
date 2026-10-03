# Python runtime

`tg py` embeds CPython using PyO3. It runs a filesystem or checked-in entry module,
loads relative Tangram modules through the server's module resolver and loader,
and executes an optional named export. `tg run` recognizes Python modules and
invokes their `default` export through this runtime.

Every Tangram Python module receives `tg` in its globals, so `tg.file(...)` and the
rest of the client API work without an import. Standalone scripts executed by
Python directly use `import tangram as tg`.

Install uv and Python 3.12 or later, then prepare the workspace and build:

```sh
uv sync --locked --all-packages
cargo build --all-features --bin tangram
```

Cargo selects `.venv/bin/python` through `.cargo/config.toml`. `PYO3_PYTHON` can
override that selection, but its major/minor version must match the workspace
interpreter. The binary links to that CPython distribution and needs its standard
library and native standard-library extensions at runtime. The Tangram client and
its locked pure Python dependencies are embedded in the binary. Distribution
packaging for CPython itself is separate work.

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
Function commands and sandboxed CPython distribution packaging are separate work.

Run integration tests with:

```sh
nu packages/cli/test.nu --no-cloud 'py/(run|runtime|relative|resolution|errors)'
```
