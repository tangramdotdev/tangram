use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'test.tg.py': 'def default():
    print("hello, world!")
'
}
let output = tg run ($path | path join test.tg.py) | complete
success $output
assert equal ($output.stdout | str trim) 'hello, world!'

let path = artifact {
    'test.tg.py': 'def default():
    return tg.file("hello, world!")
'
}
let output = tg run ($path | path join test.tg.py) | complete
success $output
let output_path = tg checkout ($output.stdout | str trim)
assert equal (open --raw $output_path) 'hello, world!'

# Checked-in module graphs retain relative dependencies without the source files.
let path = artifact {
    'tangram.py': 'pass'
    'main.tg.py': '
        from .helper import value
        from . import cycle_a, sub
        from .namespace.leaf import leaf

        async def default(*args):
            assert args == ("hello", True, "trailing")
            assert tg.process.module.kind == "python"
            assert value == 42
            assert cycle_a.result == 7
            assert sub.child == 42
            assert leaf == 9
            print("checked-in Python")
            return 17
    '
    'helper.tg.py': '
        assert tg.process.module.kind == "python"
        value = 42
    '
    'cycle_a.tg.py': '
        value = 7
        from .cycle_b import result
    '
    'cycle_b.tg.py': '
        from .cycle_a import value
        result = value
    '
    sub: {
        'tangram.py': 'from .child import child'
        'child.tg.py': '
            from ..helper import value
            child = value
        '
    }
    namespace: {'leaf.tg.py': 'leaf = 9'}
}
let checked = tg checkin ($path | path join main.tg.py)
rm --recursive $path
let output = tg run -a hello -A true $checked trailing | complete
success $output
assert equal ($output.stdout | str trim) "checked-in Python\n17"

let package = artifact {
    'tangram.py': '
        from .helper import value
        def default():
            print(value)
    '
    'helper.tg.py': 'value = "Python package"'
}
let output = tg run $package | complete
success $output
assert equal ($output.stdout | str trim) 'Python package'
