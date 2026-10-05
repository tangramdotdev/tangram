use ../lib/test.nu *

let local = server spawn

# Standalone modules have no context for relative imports, even when siblings exist.
let standalone = artifact {
    'main.tg.py': '
        assert __package__ == ""
        try:
            from .helper import value
        except ImportError as error:
            assert str(error) == "attempted relative import with no known parent package"
        else:
            assert False
        def default():
            return 42
    '
    'helper.tg.py': 'value = 42'
}
let output = tg python --export default ($standalone | path join main.tg.py) | complete
success $output
assert equal ($output.stdout | str trim) ''
let checked = tg checkin ($standalone | path join main.tg.py)
rm --recursive $standalone
let output = tg run $checked | complete
success $output
assert equal ($output.stdout | str trim) '42'

# Namespace children share the existing parent, and relative imports cannot cross the root.
let package = artifact {
    'tangram.py': '
        import importlib
        value = 42
        from .namespace import leaf
        assert leaf.parent is importlib.import_module(".", __package__)
        try:
            from .. import missing
        except ImportError as error:
            assert str(error) == "attempted relative import beyond top-level package"
        else:
            assert False
        def default():
            return leaf.value
    '
    namespace: {
        'leaf.tg.py': '
            import importlib
            from .. import value
            parent = importlib.import_module("..", __package__)
            try:
                from ... import missing
            except ImportError as error:
                assert str(error) == "attempted relative import beyond top-level package"
            else:
                assert False
    '
    }
}
let output = tg python --export default ($package | path join tangram.py) | complete
success $output
assert equal ($output.stdout | str trim) ''
let checked_file = tg checkin ($package | path join tangram.py)
let checked = tg checkin $package
rm --recursive $package
let output = tg run $checked | complete
success $output
assert equal ($output.stdout | str trim) '42'

let output = tg run $checked_file | complete
success $output
assert equal ($output.stdout | str trim) '42'

# Recorded dependencies cannot grant a package parent outside its directory referent.
let builder = artifact {
    'tangram.ts': '
        export default async function () {
            const outside = await tg.file("raise AssertionError(\"outside the artifact root\")").module("python");
            const source = "try:\n    from .. import value\nexcept ImportError as error:\n    assert str(error) == \"attempted relative import beyond top-level package\"\nelse:\n    assert False\ndef default():\n    return 42";
            const root = await tg.file(source).module("python").dependency("../tangram.py", { node: outside });
            return await tg.directory({ "tangram.py": root });
        }
    '
}
let module = tg run $builder
let output = tg run $module | complete
success $output
assert equal ($output.stdout | str trim) '42'
