"""Build the standalone client with the installed Python interpreter."""

import argparse
import contextlib
import functools
import hashlib
import os
import shutil
import subprocess
import sys
import sysconfig
import tempfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="release")
    args = parser.parse_args()
    if sys.version_info < (3, 12):  # noqa: UP036
        parser.error("the python client requires python 3.12 or later")
    directory = Path(__file__).resolve().parent
    root = directory.parents[2]
    venv = root / ".venv"
    python = venv / "bin/python"
    uv = shutil.which("uv")
    if uv is None:
        parser.error("the python workspace requires uv")
    subprocess.run(
        [
            uv,
            "sync",
            "--locked",
            "--all-packages",
            "--no-install-package",
            "tangram",
            "--python",
            sys.executable,
            "--no-managed-python",
        ],
        cwd=root,
        check=True,
    )
    # Build the extension for its host interpreter rather than the embedded runtime.
    with environment():
        subprocess.run(
            [str(python), "-m", "maturin", "develop", "--profile", args.profile],
            cwd=directory,
            env={
                **os.environ,
                "VIRTUAL_ENV": str(venv),
            },
            check=True,
        )


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
        f"ext_suffix={sysconfig.get_config_var('EXT_SUFFIX')}\n"
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
    # The custom backend delegates to maturin after configuring the host interpreter.
    variables = {
        "MATURIN_NO_MISSING_BUILD_BACKEND_WARNING": "1",
        "PYO3_CONFIG_FILE": str(config),
    }
    previous = {name: os.environ.get(name) for name in variables}
    os.environ.update(variables)
    try:
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


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


if __name__ == "__main__":
    main()
