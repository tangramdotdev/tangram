use ../lib/test.nu *

let local = server spawn
let tools = artifact {
    'tangram.py': 'value = 7'
    'child.tg.py': '
        import importlib
        from . import value as parent_value
        parent = importlib.import_module(".", __package__)
        value = 8
    '
    namespace: {'leaf.tg.py': 'value = 9'}
    'cycle_a.tg.py': '
        # /// script
        # [tool.tangram.imports.other]
        # specifier = "./cycle_b.tg.py"
        # ///
        value = "a"
        import other
        assert other.value == "b"
    '
    'cycle_b.tg.py': '
        # /// script
        # [tool.tangram.imports.other]
        # specifier = "./cycle_a.tg.py"
        # ///
        value = "b"
        import other
        assert other.value == "a"
    '
    'flaky.tg.py': '
        tg._retry_attempts += 1
        if tg._retry_attempts == 1:
            raise ValueError("retry")
        value = 10
    ' 
    debug: {
        'tangram.py': 'from .helper import value; calls = globals().get("calls", 0) + 1'
        'helper.tg.py': 'value = 42'
    }
    release: {
        'tangram.py': 'from .helper import value'
        'helper.tg.py': 'value = 43'
    }
}
tg tag put -p tools/1.0.0 $tools
let path = artifact {
    'tangram.py': 'pass'
    'main.tg.py': '
        # /// script
        # requires-python = ">=3.14,<3.15"
        # [tool.tangram.imports.alternate]
        # specifier = "./left.tg.py"
        # attributes = { source = "./right.tg.py" }
        # [tool.tangram.imports.debug]
        # specifier = "tools/^1"
        # attributes = { get = "debug/tangram.py" }
        # [tool.tangram.imports.same]
        # specifier = "tools/^1"
        # attributes = { get = "debug/tangram.py" }
        # [tool.tangram.imports.release]
        # specifier = "tools/^1"
        # attributes = { get = "release/tangram.py" }
        # [tool.tangram.imports.cycle]
        # specifier = "tools/^1"
        # attributes = { get = "cycle_a.tg.py" }
        # [tool.tangram.imports.flaky]
        # specifier = "tools/^1"
        # attributes = { get = "flaky.tg.py" }
        # [tool.tangram.imports.cycle_asset]
        # specifier = "tools/^1"
        # attributes = { get = "cycle_a.tg.py", type = "file" }
        # [tool.tangram.imports.asset]
        # specifier = "tools/^1"
        # attributes = { get = "child.tg.py", type = "file" }
        # [tool.tangram.imports.local_child]
        # specifier = "./local/child.tg.py"
        # [tool.tangram.imports.direct_child]
        # specifier = "tools/^1"
        # attributes = { get = "child.tg.py" }
        # [tool.tangram.imports.pkg]
        # specifier = "tools/^1"
        # ///

        import importlib
        import importlib.util
        import math
        import debug as development
        import alternate
        import same
        import cycle
        from release import value as production
        import local_child
        import direct_child
        import pkg
        import pkg.child as child
        from pkg import value
        from asset import default as asset
        from cycle_asset import default as cycle_asset
        import pkg.namespace.leaf as leaf
        from . import left, right

        async def default():
            assert development is same
            assert same.calls == 1
            assert development.value == 42 and production == 43
            assert value == 7 and child.value == 8 and leaf.value == 9
            assert local_child.value == 11
            assert direct_child is child
            assert child.parent_value == 7 and child.parent is pkg
            assert importlib.import_module("debug") is development
            assert __import__("same") is same
            assert importlib.util.find_spec("debug") is development.__spec__
            assert left.read() == 42 and right.read() == 43
            assert alternate.read() == 43
            assert math.sqrt(4) == 2
            assert tg.File.is_(asset)
            assert "value = 8" in await asset.text()
            assert "value = \"a\"" in await cycle_asset.text()
            assert cycle.other.other.value == "a"
            assert cycle.other.other.other is cycle.other
            tg._retry_attempts = 0
            try:
                importlib.import_module("flaky")
            except ValueError as error:
                assert str(error) == "retry"
            else:
                assert False, "expected the first import to fail"
            assert importlib.import_module("flaky").value == 10
            assert tg._retry_attempts == 2
            print("declared imports completed")
    '
    local: {
        'tangram.py': 'value = 11'
        'child.tg.py': 'from . import value'
    }
    'left.tg.py': '
        # /// script
        # [tool.tangram.imports.common]
        # specifier = "tools/^1"
        # attributes = { get = "debug/tangram.py" }
        # ///
        import importlib
        def read():
            return importlib.import_module("common").value
    '
    'right.tg.py': '
        # /// script
        # [tool.tangram.imports.common]
        # specifier = "tools/^1"
        # attributes = { get = "release/tangram.py" }
        # ///
        def read():
            from common import value
            return value
    '
}
let output = tg python --export default ($path | path join main.tg.py) | complete
success $output
assert equal ($output.stdout | str trim) 'declared imports completed'
let checked = tg checkin ($path | path join main.tg.py)
rm --recursive $path
rm --recursive $tools
let output = tg run $checked | complete
success $output
assert equal ($output.stdout | str trim) 'declared imports completed'
