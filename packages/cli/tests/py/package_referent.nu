use ../lib/test.nu *

let local = server spawn
let package = artifact {
    'tangram.py': '
        value = 42
        from .namespace import leaf
        def default():
            return value
    '
    namespace: {
        'leaf.tg.py': '
            import importlib
            from .. import value
            parent = importlib.import_module("..", __package__)
            namespace = importlib.import_module(".", __package__)
            calls = globals().get("calls", 0) + 1
            assert calls == 1 and value == 42
            def read():
                from .. import value
                return value
            def default():
                return read()
        '
    }
}
tg tag put -p tools/1.0.0 $package
let checked_package = tg checkin $package
mut checked = []
for imports in ["import leaf\nimport pkg.namespace.leaf" "import pkg.namespace.leaf\nimport leaf"] {
    let source = '# /// script
# [tool.tangram.imports.leaf]
# specifier = "tools/^1"
# attributes = { get = "namespace/leaf.tg.py" }
# [tool.tangram.imports.pkg]
# specifier = "tools/^1"
# ///
' + $imports + '
assert leaf is pkg.namespace.leaf
assert leaf.parent is pkg
assert leaf.namespace is pkg.namespace
assert leaf.calls == 1
def default():
    return leaf.read()
'
    let entry = artifact {'main.tg.py': $source}
    let output = tg py --export default ($entry | path join main.tg.py) | complete
    success $output
    $checked = $checked | append (tg checkin ($entry | path join main.tg.py))
    rm --recursive $entry
}
rm --recursive $package
for module in $checked {
    let output = tg run $module | complete
    success $output
    assert equal ($output.stdout | str trim) '42'
}
for reference in [$checked_package ($checked_package + '&get=namespace/leaf.tg.py')] {
    let output = tg run $reference | complete
    success $output
    assert equal ($output.stdout | str trim) '42'
}
