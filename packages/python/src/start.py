"""Entry point called by Rust runtime.

This module mirrors the JavaScript packages/js/src/start.ts.
It receives the argument dict and root module from Rust,
then calls the export function and returns the result.
"""

from typing import Any, Dict

import tangram as tg


async def start(arg: Dict[str, Any], root_module: Any) -> Any:
    """Start the execution.

    Args:
        arg: Dictionary containing args, cwd, env, and executable.
        root_module: The imported root module.

    Returns:
        The serialized output value.
    """
    # Set up tg.process.
    args = [tg.from_data(a) for a in arg.get("args", [])]
    cwd = arg.get("cwd", "")
    env = {k: tg.from_data(v) for k, v in arg.get("env", {}).items()}
    executable = arg.get("executable", {})

    tg.set_process({
        "args": args,
        "cwd": cwd,
        "env": env,
        "executable": executable,
    })

    # Set tg.Process.current from the TANGRAM_PROCESS environment variable if it is defined.
    tangram_process = env.get("TANGRAM_PROCESS")
    if tangram_process is not None and isinstance(tangram_process, str):
        tg.Process.current = tg.Process(tangram_process)

    # Get the export name from executable.
    export_name = executable.get("export", "default") if isinstance(executable, dict) else "default"
    if export_name is None:
        export_name = "default"

    # If there is no export, return None.
    if isinstance(executable, dict) and "export" in executable and executable["export"] is None:
        return None

    # Check if the export exists.
    if not hasattr(root_module, export_name):
        raise RuntimeError(f"failed to find the export named {export_name}")

    # Get the export value.
    value = getattr(root_module, export_name)

    # If it is a function, call it with spread args (matching JS behavior).
    if callable(value):
        output = await tg.resolve(value(*args))
    elif tg.is_value(value):
        output = value
    else:
        raise RuntimeError("the export must be a tg.Value or a function")

    # Store and serialize the output.
    result = await tg.store_and_serialize(output)
    return result
