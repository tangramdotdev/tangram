use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'tangram.py': 'pass'
    'main.tg.py': '
        import importlib
        from .left import entry as left
        from .right import entry as right
        assert left.value == 1
        assert right.value == 2
        assert left.module is not right.module
        for _ in range(2):
            try:
                importlib.import_module(".broken", __package__)
            except RuntimeError as error:
                assert str(error) == "failed initialization"
            else:
                assert False
        print("resolution completed")
    '
    left: {
        'entry.tg.py': 'from . import helper as module; value = module.value'
        'helper.tg.py': 'value = 1'
    }
    right: {
        'entry.tg.py': 'from . import helper as module; value = module.value'
        'helper.tg.py': 'value = 2'
    }
    'broken.tg.py': 'raise RuntimeError("failed initialization")'
}
let output = tg py ($path | path join main.tg.py) | complete
success $output
assert equal ($output.stdout | str trim) 'resolution completed'

let missing = artifact {'tangram.py': 'pass', 'main.tg.py': 'from . import missing'}
let output = tg py ($missing | path join main.tg.py) | complete
failure $output
assert ($output.stderr | str contains 'missing')

let ambiguous = artifact {
    'tangram.py': 'pass'
    'main.tg.py': 'from . import helper'
    'helper.tg.py': 'value = 1'
    helper: {'tangram.py': 'value = 2'}
}
let output = tg py ($ambiguous | path join main.tg.py) | complete
failure $output
assert ($output.stderr | str contains 'ambiguous Tangram module')

# Exercise the same package tree through every entry and import route.
let package = artifact {
    'tangram.py': '
        value = 42
        calls = globals().get("calls", 0) + 1
        from . import sub
        def default():
            assert calls == sub.calls == sub.child.calls == 1
            assert sub.result == 50
            assert sub.child.root_value == value
            return value
    '
    'value.tg.py': 'raise RuntimeError("a sibling must not replace a package export")'
    sub: {
        'tangram.py': '
            from .. import value
            local = 7
            calls = globals().get("calls", 0) + 1
            from . import child
            result = value + child.value
            def default():
                assert value == child.root_value == 42
                assert local == child.local == 7
                assert calls == child.calls == 1
                return value
        '
        'child.tg.py': '
            import importlib
            from .. import value as root_value
            from . import local
            parent = importlib.import_module(".", __package__)
            root = importlib.import_module("..", __package__)
            calls = globals().get("calls", 0) + 1
            value = 8
            def default():
                assert root_value == root.value == 42
                assert local == parent.local == 7
                assert calls == parent.calls == root.calls == 1
                return root_value
        '
    }
}
tg tag put -p tools/1.0.0 $package
let namespace = artifact {
    'tangram.py': 'pass'
    'left.tg.py': 'import importlib; parent = importlib.import_module(".", __package__)'
    'right.tg.py': 'import importlib; parent = importlib.import_module(".", __package__)'
}
tg tag put -p namespace/1.0.0 $namespace
let entry = artifact {
    'main.tg.py': '
        # /// script
        # [tool.tangram.imports.left]
        # specifier = "namespace/^1"
        # attributes = { get = "left.tg.py" }
        # [tool.tangram.imports.right]
        # specifier = "namespace/^1"
        # attributes = { get = "right.tg.py" }
        # [tool.tangram.imports.unloaded]
        # specifier = "tools/^1"
        # attributes = { get = "value.tg.py" }
        # [tool.tangram.imports.child]
        # specifier = "tools/^1"
        # attributes = { get = "sub/child.tg.py" }
        # [tool.tangram.imports.same]
        # specifier = "tools/^1"
        # attributes = { get = "sub/child.tg.py" }
        # [tool.tangram.imports.sub]
        # specifier = "tools/^1"
        # attributes = { get = "sub/tangram.py" }
        # [tool.tangram.imports.pkg]
        # specifier = "tools/^1"
        # ///
        import importlib
        import child, same, sub, pkg, left, right
        def default():
            assert importlib.util.find_spec("unloaded") is importlib.util.find_spec("pkg.value")
            assert pkg.value == 42
            assert left.parent is right.parent
            assert child is same is sub.child is pkg.sub.child
            assert sub is pkg.sub
            assert child.parent is sub and child.root is pkg
            assert importlib.import_module("child") is child
            assert importlib.util.find_spec("same") is child.__spec__
            assert child.calls == sub.calls == pkg.calls == 1
            return child.root_value
    '
}
for module in ['tangram.py' 'sub/tangram.py' 'sub/child.tg.py'] {
    let output = tg py --export default ($package | path join $module) | complete
    success $output
}
let output = tg py --export default ($entry | path join main.tg.py) | complete
success $output
let checked = ['tangram.py' 'sub/tangram.py' 'sub/child.tg.py'] | each {|module| tg checkin ($package | path join $module)}
let checked_entry = tg checkin ($entry | path join main.tg.py)
rm --recursive $package
rm --recursive $entry
rm --recursive $namespace
for module in ($checked | append $checked_entry) {
    let output = tg run $module | complete
    success $output
    assert equal ($output.stdout | str trim) '42'
}
