use ../lib/test.nu *

let local = server spawn
let package = artifact {
    'tangram.py': '
        value = 42
        from .child import read_child
        import importlib
        async def read():
            return importlib.import_module(".late", __package__).value
    '
    'late.tg.py': 'value = 42'
    'child.tg.py': '
        import importlib
        from . import value
        parent = importlib.import_module(".", __package__)
        async def read_child():
            assert parent.value == value
            return value
    '
}
tg tag put -p tools/1.0.0 $package
let path = artifact {
    'tangram.py': '
        # /// script
        # [tool.tangram.imports.pkg]
        # specifier = "tools/^1"
        # ///
        import asyncio
        import functools
        import pkg
        from .helper import child

        @functools.cache
        def original(value):
            return value + " sync"
        renamed = original
        del original

        async def default():
            assert await tg.command(pkg.read).build() == 42
            assert await tg.command(pkg.read_child).build() == 42
            referent = await tg.Command.python(pkg.read, [])
            module = (await referent.node.args)[3].value
            assert all(module.referent.options.get(key) is None for key in ("id", "name", "path", "tag"))
            assert tg.host.magic(renamed)["export"] == "renamed"
            assert await tg.command(renamed, "hello").build() == "hello sync"
            for flag, value, expected in [("-a", tg.Command.Value.string("raw"), "raw"), ("-A", tg.Command.Value.value(42), 42)]:
                assert await tg.command(child, tg.Command.Value.string(flag), value).build() == [flag, expected]
                assert await tg.command(child).arg(tg.Command.Value.string(flag), value).build() == [flag, expected]
                assert await tg.command(child).build().arg(tg.Command.Value.string(flag), value) == [flag, expected]
                assert await tg.command(child).arg(tg.Command.Value.string(flag), value).build().run() == [flag, expected]
                referent = await tg.Command.python(child, [tg.Command.Value.string(flag), value])
                assert await referent.node.run() == [flag, expected]
            shared = asyncio.sleep(0, result={"value": 42})
            builder = tg.command(child, shared).arg(shared, tg.Command.Value.string("raw"))
            output = await builder.build()
            assert output == [{"value": 42}, {"value": 42}, "raw"]
            output = await builder.build().arg(True)
            assert output == [{"value": 42}, {"value": 42}, "raw", True]
            referent = await tg.Command.python(child, [None])
            assert await referent.node.run().arg("extra") == [None, "extra"]
            print("Python function commands completed")
    '
    'helper.tg.py': '
        async def child(*args):
            return list(args)
    '
}
let output = tg python --export default ($path | path join tangram.py) | complete
success $output
assert equal ($output.stdout | str trim) 'Python function commands completed'
let checked = tg checkin $path
rm --recursive $path
rm --recursive $package
let output = tg run $checked | complete
success $output
assert equal ($output.stdout | str trim) 'Python function commands completed'

# Function commands and file entries must share their package initializer with its children.
let path = artifact {
    'tangram.py': '
        import importlib
        value = 42
        from .namespace import leaf
        assert leaf.parent is importlib.import_module(".", __package__)
        def task():
            assert tg.process.module.to_data()["referent"]["node"] == __tangram_module__.to_data()["referent"]["node"]
            return value
        async def default():
            assert await tg.command(task).build() == 42
    '
    namespace: {
        'leaf.tg.py': '
            import importlib
            from .. import value
            parent = importlib.import_module("..", __package__)
        '
    }
}
let output = tg python --export default ($path | path join tangram.py) | complete
success $output
let checked = tg checkin ($path | path join tangram.py)
rm --recursive $path
let output = tg run $checked | complete
success $output
