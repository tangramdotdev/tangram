use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'tangram.py': '
        # /// script
        # [tool.tangram.imports.alias]
        # specifier = "./package"
        # ///
        import alias.child
        import alias.nested.leaf
        from alias import child
        from . import sibling
        value: int = alias.child.value + child.value + sibling.value + alias.nested.leaf.value
    '
    'sibling.tg.py': 'value: int = 3'
    package: {
        'tangram.py': 'pass'
        'child.tg.py': 'value: int = 4'
        nested: {
            'tangram.py': 'pass'
            'leaf.tg.py': 'value: int = 5'
        }
    }
}
success (tg check $path | complete)
' value: str = "wrong"' | str trim | save --force ($path | path join package child.tg.py)
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains 'tangram.py:')

# A single-file module cannot be traversed as a package.
let path = artifact {
    'tangram.py': 'from .child.leaf import value'
    'child.tg.py': 'pass'
    child: {'leaf.tg.py': 'value: int = 1'}
}
failure (tg check $path | complete)
