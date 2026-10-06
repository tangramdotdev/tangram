use ../lib/test.nu *

let local = server spawn
let package = artifact {
    'tangram.py': '
        value: int = 42
        from .namespace import leaf
        def default() -> int:
            return leaf.read()
    '
    namespace: {
        'leaf.tg.py': '
            from .. import value
            from .nested import other
            def read() -> int:
                return value + other.value
        '
        nested: {'other.tg.py': 'value: int = 1'}
    }
}

# The checker and runtime resolve the same namespace ancestry from paths and artifacts.
let file = $package | path join tangram.py
success (tg check $file | complete)
success (tg python --export default $file | complete)
let checked_file = tg checkin $file
let checked_package = tg checkin $package
tg tag put -p namespace/1.0.0 $package
let entry = artifact {
    'main.tg.py': '
        # /// script
        # [tool.tangram.imports.pkg]
        # specifier = "namespace/^1"
        # [tool.tangram.imports.leaf]
        # specifier = "namespace/^1"
        # attributes = { get = "namespace/leaf.tg.py" }
        # ///
        import leaf
        import pkg.namespace.leaf
        assert leaf is pkg.namespace.leaf
        def default() -> int:
            return leaf.read()
    '
}
let entry = $entry | path join main.tg.py
success (tg check $entry | complete)
success (tg python --export default $entry | complete)
let checked_entry = tg checkin $entry
rm --recursive $package
rm $entry
for module in [$checked_file $checked_package $checked_entry] {
    success (tg check $module | complete)
    let output = tg run $module | complete
    success $output
    assert equal ($output.stdout | str trim) '43'
}

# Namespace imports retain concrete types rather than silently becoming unknown.
let invalid = artifact {
    'tangram.py': 'from .namespace import leaf; value: str = leaf.value'
    namespace: {'leaf.tg.py': 'value: int = 42'}
}
failure (tg check $invalid | complete)

# Both consumers enforce the same top-level package boundary through namespaces.
let invalid = artifact {
    'tangram.py': 'from .namespace import leaf'
    namespace: {'leaf.tg.py': 'from ... import outside'}
}
failure (tg check $invalid | complete)
failure (tg python ($invalid | path join tangram.py) | complete)
