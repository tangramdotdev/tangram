"""Prepare the native interpreter, frozen bootstrap, and embedded library."""

import csv
import importlib.metadata
import json
import runpy
import shutil
import subprocess
import sys
import tomllib
from email.message import Message
from pathlib import Path

from llvm import Compiler
from native import namespace


def main():
    distribution, host, output, client, sysroot, packages = map(Path, sys.argv[1:])
    sys.path.insert(0, str(packages))
    metadata = json.loads((distribution / "PYTHON.json").read_text())
    host_metadata = json.loads((host / "PYTHON.json").read_text())
    stdlib = distribution / metadata["python_paths"]["stdlib"]

    # Copy the library without test suites, GUI support, or installation tools.
    library = output / "library"
    if library.exists():
        shutil.rmtree(library)
    library.mkdir()
    excluded = {
        "__pycache__",
        "site-packages",
        "test",
        "tests",
        "idlelib",
        "tkinter",
        "turtledemo",
        "ensurepip",
        "lib-dynload",
        "config-3.14-darwin",
        "config-3.14-x86_64-linux-gnu",
        "config-3.14-aarch64-linux-gnu",
    }
    for path in sorted(stdlib.rglob("*")):
        relative = path.relative_to(stdlib)
        if any(
            part in excluded or part.startswith("config-3.14-")
            for part in relative.parts
        ):
            continue
        if not path.is_file() or path.suffix in {".pyc", ".so", ".a", ".dylib"}:
            continue
        target = library / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)
    runpy.run_path(str(client.parent / "library.py"))["copy"](library)

    # Preserve distribution metadata for version checks and entry point discovery.
    for name in ["h2", "hpack", "hyperframe", "tomli-w", "PyYAML"]:
        dependency = importlib.metadata.distribution(name)
        for path in dependency.files or []:
            if len(path.parts) < 2 or not path.parts[0].endswith(".dist-info"):
                continue
            target = library / path
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(dependency.locate_file(path), target)

    # Describe the source client independently of an installed native extension.
    project = tomllib.loads((client.parent / "pyproject.toml").read_text())["project"]
    directory = library / f"{project['name']}-{project['version']}.dist-info"
    directory.mkdir()
    metadata_text = Message()
    for name, value in [
        ("Metadata-Version", "2.4"),
        ("Name", project["name"]),
        ("Version", project["version"]),
        ("Summary", project["description"]),
        ("Requires-Python", project["requires-python"]),
        ("License-Expression", project["license"]),
    ]:
        metadata_text[name] = value
    for dependency in project["dependencies"]:
        metadata_text["Requires-Dist"] = dependency
    (directory / "METADATA").write_text(str(metadata_text), encoding="utf-8")
    (directory / "top_level.txt").write_text("tangram\n", encoding="utf-8")
    record = directory / "RECORD"
    paths = sorted(
        path.relative_to(library).as_posix()
        for root in [library / "tangram", directory]
        for path in root.rglob("*")
        if path.is_file()
    )
    with record.open("w", newline="", encoding="utf-8") as stream:
        csv.writer(stream).writerows(
            (path, "", "") for path in [*paths, record.relative_to(library).as_posix()]
        )

    # Freeze only the modules required before the embedded loader is installed.
    subprocess.run(
        [
            str(host / host_metadata["python_exe"]),
            "-I",
            "freeze.py",
            str(stdlib),
            str(output),
            metadata["python_version"],
        ],
        check=True,
    )
    frozen = json.loads((output / "frozen.json").read_text())
    for _, _, _, relative in frozen:
        (library / relative).unlink()

    # Replace the built-in initialization table with the supported native modules.
    build = metadata["build_info"]
    extensions = {
        name: variants[0]
        for name, variants in build["extensions"].items()
        if not name.startswith(("_test", "_xx"))
        and name
        not in {"xxsubtype", "_tkinter", "_curses", "_curses_panel", "readline"}
    }
    functions = sorted({entry["init_fn"] for entry in extensions.values()} - {"NULL"})
    source = (
        '#define Py_BUILD_CORE\n#include "Python.h"\n'
        '#include "internal/pycore_import.h"\n#include <stdio.h>\n'
    )
    source += "".join(f"extern PyObject* {name}(void);\n" for name in functions)
    source += "struct _inittab _PyImport_Inittab[] = {\n"
    source += "".join(
        f'{{"{name}", {entry["init_fn"]}}},\n'
        for name, entry in sorted(extensions.items())
    )
    source += "{NULL, NULL}};\n"
    source += "".join(f'#include "frozen_{index}.h"\n' for _, index, _, _ in frozen)
    for table, bootstrap in [("bootstrap", True), ("frozen", False)]:
        source += f"static const struct _frozen {table}[] = {{\n"
        source += "".join(
            f'{{"{name}", frozen_{index}, sizeof(frozen_{index}), {int(package)}}},\n'
            for name, index, package, _ in frozen
            if (name.startswith("_frozen_importlib") or name == "zipimport")
            == bootstrap
        )
        source += "{NULL, NULL, 0, 0}};\n"
    source += r"""
static const struct _frozen empty[] = {{NULL, NULL, 0, 0}};
const struct _frozen *_PyImport_FrozenBootstrap = bootstrap;
const struct _frozen *_PyImport_FrozenStdlib = empty;
const struct _frozen *_PyImport_FrozenTest = empty;
const struct _frozen *PyImport_FrozenModules = frozen;
static const struct _module_alias aliases[] = {
    {"_frozen_importlib", "importlib._bootstrap"},
    {"_frozen_importlib_external", "importlib._bootstrap_external"},
    {NULL, NULL}
};
const struct _module_alias *_PyImport_FrozenAliases = aliases;
"""
    source += r"""
int tangram_python_initialize(char *error, size_t capacity) {
    if (Py_IsInitialized()) {
        snprintf(error, capacity, "the Python interpreter was already initialized");
        return -1;
    }
    PyImport_FrozenModules = frozen;
    PyPreConfig preconfig;
    PyPreConfig_InitIsolatedConfig(&preconfig);
    preconfig.utf8_mode = 1;
    PyStatus status = Py_PreInitialize(&preconfig);
    if (PyStatus_Exception(status)) {
        snprintf(error, capacity, "%s", status.err_msg ? status.err_msg :
            "failed to preinitialize Python");
        return -1;
    }
    PyConfig config;
    PyConfig_InitIsolatedConfig(&config);
    config.configure_c_stdio = 0;
    config.install_signal_handlers = 0;
    config.parse_argv = 0;
    config.site_import = 0;
    config.module_search_paths_set = 1;
    config.use_frozen_modules = 1;
    status = PyConfig_SetBytesString(&config, &config.program_name, "/tangram/bin/tg");
    if (!PyStatus_Exception(status)) {
        status = PyConfig_SetBytesString(&config, &config.home, "/tangram/python");
    }
    if (!PyStatus_Exception(status)) {
        status = Py_InitializeFromConfig(&config);
    }
    if (PyStatus_Exception(status)) {
        snprintf(error, capacity, "%s", status.err_msg ? status.err_msg :
            "failed to initialize Python");
        PyConfig_Clear(&config);
        return -1;
    }
    PyConfig_Clear(&config);
    PyEval_SaveThread();
    return 0;
}
"""
    (output / "python.c").write_text(source)
    objects = set(build["core"]["objs"]) - {
        build["inittab_object"],
        "build/Python/frozen.o",
    }
    links = list(build["core"]["links"])
    for extension in extensions.values():
        objects.update(extension["objs"])
        links.extend(extension["links"])
    compiler = None
    native = []
    for relative in sorted(objects):
        path = distribution / relative
        if path.read_bytes()[:4] in {b"\xde\xc0\x17\x0b", b"BC\xc0\xde"}:
            if compiler is None:
                compiler = Compiler(sysroot, metadata["target_triple"])
            target = output / "objects" / relative
            compiler.compile(path, target)
            path = target
        else:
            path = namespace(
                path, output / "objects" / relative, metadata["target_triple"], sysroot
            )
        native.append(str(path))
    (output / "native.json").write_text(json.dumps({"objects": native, "links": links}))


if __name__ == "__main__":
    main()
