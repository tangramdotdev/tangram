"""POSIX paths with the same component handling as the JavaScript client."""


class Component:
    Current = "."
    Parent = ".."
    Root = "/"
    current = Current
    parent = Parent
    root = Root

    @staticmethod
    def is_normal(component: str) -> bool:
        return component not in (Component.Current, Component.Parent, Component.Root)


def components(arg: str) -> list[str]:
    output = arg.split("/")
    if not output[0]:
        output[0] = Component.Root
    return [
        item
        for index, item in enumerate(output)
        if item and not (index > 0 and item == Component.Current)
    ]


def from_components(values: list[str]) -> str:
    return (
        "/" + "/".join(values[1:])
        if values and values[0] == Component.Root
        else "/".join(values)
    )


def is_absolute(arg: str) -> bool:
    return arg.startswith("/")


def join(*args: str | None) -> str:
    output = []
    for arg in args:
        if arg is None:
            continue
        output = components(arg) if is_absolute(arg) else output + components(arg)
    return from_components(output)


def parent(arg: str) -> str | None:
    values = components(arg)
    return from_components(values[:-1]) if values else None
