"""Build native client extensions using the host interpreter's PyO3 configuration."""

import contextlib
import functools
import hashlib
import os
import sys
import sysconfig
import tempfile
from pathlib import Path


@contextlib.contextmanager
def environment():
    flags = [
        name
        for name in ["Py_DEBUG", "Py_GIL_DISABLED", "Py_TRACE_REFS"]
        if sysconfig.get_config_var(name)
    ]
    implementation = {"cpython": "CPython", "pypy": "PyPy"}[sys.implementation.name]
    contents = (
        f"implementation={implementation}\n"
        f"version={sys.version_info.major}.{sys.version_info.minor}\n"
        f"shared=true\nbuild_flags={','.join(flags)}\n"
        f"pointer_width={sysconfig.get_config_var('SIZEOF_VOID_P') * 8}\n"
        "suppress_build_script_link_lines=true\n"
    ).encode()
    directory = Path(__file__).resolve().parent
    workspace = next(
        (parent for parent in directory.parents if (parent / "Cargo.toml").is_file()),
        directory / "native",
    )
    target = Path(os.environ.get("CARGO_TARGET_DIR", workspace / "target")).resolve()
    cache = target / "pyo3"
    cache.mkdir(parents=True, exist_ok=True)
    config = cache / f"{hashlib.sha256(contents).hexdigest()}.txt"
    if not config.exists():
        # Publish complete configurations without changing existing files' timestamps.
        with tempfile.TemporaryDirectory(dir=cache) as temporary:
            pending = Path(temporary) / "pyo3.txt"
            pending.write_bytes(contents)
            try:
                os.link(pending, config)
            except FileExistsError:
                pass
    previous = os.environ.get("PYO3_CONFIG_FILE")
    os.environ["PYO3_CONFIG_FILE"] = str(config)
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("PYO3_CONFIG_FILE", None)
        else:
            os.environ["PYO3_CONFIG_FILE"] = previous


def __getattr__(name):
    import maturin

    hook = getattr(maturin, name)
    if not callable(hook):
        return hook

    @functools.wraps(hook)
    def invoke(*args, **kwargs):
        with environment():
            return hook(*args, **kwargs)

    return invoke
