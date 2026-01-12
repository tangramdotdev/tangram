"""Path utilities matching JS path.ts."""

from typing import List, Optional, Union

# Component types.
CURRENT = "."
PARENT = ".."
ROOT = "/"

Component = str


def is_normal(component: Component) -> bool:
    """Check if a component is a normal path segment."""
    return component not in (CURRENT, PARENT, ROOT)


def components(arg: str) -> List[Component]:
    """Split a path into its components."""
    parts = arg.split("/")
    result: List[Component] = []

    # Handle root.
    if parts and len(parts[0]) == 0:
        result.append(ROOT)
        parts = parts[1:]

    for i, part in enumerate(parts):
        # Skip empty parts.
        if len(part) == 0:
            continue
        # Skip . except at the beginning.
        if part == CURRENT and i > 0:
            continue
        result.append(part)

    return result


def from_components(comps: List[Component]) -> str:
    """Join components back into a path string."""
    if comps and comps[0] == ROOT:
        return "/" + "/".join(comps[1:])
    return "/".join(comps)


def is_absolute(arg: str) -> bool:
    """Check if a path is absolute."""
    return arg.startswith("/")


def join(*args: Optional[str]) -> str:
    """Join path segments together."""
    result: List[Component] = []

    for arg in args:
        if arg is None:
            continue
        if is_absolute(arg):
            result = components(arg)
        else:
            result = result + components(arg)

    return from_components(result)


def parent(arg: str) -> Optional[str]:
    """Get the parent directory of a path."""
    comps = components(arg)
    if not comps:
        return None
    return "/".join(comps[:-1]) if comps[:-1] else None


def basename(arg: str) -> str:
    """Get the last component of a path."""
    comps = components(arg)
    if not comps:
        return ""
    last = comps[-1]
    return "" if last == ROOT else last


def dirname(arg: str) -> str:
    """Get the directory name of a path."""
    p = parent(arg)
    return p if p is not None else ""


def extname(arg: str) -> str:
    """Get the file extension of a path."""
    base = basename(arg)
    if "." in base:
        return "." + base.rsplit(".", 1)[-1]
    return ""


def normalize(arg: str) -> str:
    """Normalize a path by resolving . and .. components."""
    comps = components(arg)
    result: List[Component] = []
    is_abs = comps and comps[0] == ROOT

    for comp in comps:
        if comp == ROOT:
            result = [ROOT]
        elif comp == CURRENT:
            continue
        elif comp == PARENT:
            if result and result[-1] not in (ROOT, PARENT):
                result.pop()
            elif not is_abs:
                result.append(PARENT)
        else:
            result.append(comp)

    return from_components(result)
