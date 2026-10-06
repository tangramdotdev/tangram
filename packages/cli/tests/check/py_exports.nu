use ../lib/test.nu *

let local = server spawn

# Declared aliases take precedence over ordinary libraries, including intrinsic names.
for name in [sys types typing_extensions builtins] {
    let source = ([
        '# /// script'
        ('# [tool.tangram.imports.' + $name + ']')
        '# specifier = "./dep.tg.py"'
        '# ///'
        ('import ' + $name)
        ('value: int = ' + $name + '.value')
        'def default() -> int:'
        '    return value'
    ] | str join (char nl))
    let path = artifact {'main.tg.py': $source, 'dep.tg.py': 'value: int = 42'}
    let file = $path | path join main.tg.py
    success (tg check $file | complete)
    let module = tg checkin $file
    rm --recursive $path
    success (tg check $module | complete)
    let output = tg run $module | complete
    success $output
    assert equal ($output.stdout | str trim) '42'
}

# Existing library exports take precedence over same-named sibling modules.
do {
    let import = 'from . import helper'
    let path = artifact {
        'tangram.py': 'import json as helper; __all__ = ["helper"]; from . import left; default = left.default'
        'helper.tg.py': 'value: int = 42'
        'left.tg.py': ($import + (char nl) + 'def default() -> str:' + (char nl) + '    return helper.dumps(42)')
    }
    success (tg check $path | complete)
    let module = tg checkin $path
    rm --recursive $path
    success (tg check $module | complete)
    let output = tg run $module | complete
    success $output
    assert equal ($output.stdout | str trim) '"42"'
}

# An existing Tangram export remains usable when there is no sibling dependency.
let path = artifact {
    'tangram.py': 'from . import other as helper; from . import left; default = left.default'
    'other.tg.py': 'value: int = 42'
    'left.tg.py': 'from . import helper
def default() -> int:
    return helper.value'
}
success (tg check $path | complete)
let output = tg run $path | complete
success $output
assert equal ($output.stdout | str trim) '42'

# A recorded unresolved edge is authoritative even when an initializer exports that name.
do {
    let import = 'from . import helper'
    let source = $import + (char nl) + 'value: bool = helper.value' + (char nl) + 'other: int = "wrong"'
    let builder = artifact {
        'tangram.ts': ('export default async function () {
            const member = await tg.file("value: bool = False").module("py");
            const graph = await tg.graph({ nodes: [
                { kind: "file", module: "py", contents: "from . import helper\n__all__ = [\"helper\"]\nfrom . import left", dependencies: { "./tangram.py": 0, "./helper.tg.py": member, "./left.tg.py": 1 } },
                { kind: "file", module: "py", contents: ' + ($source | to json -r) + ', dependencies: { "./left.tg.py": 1, "./tangram.py": 0, "./helper.tg.py": null } },
            ] });
            return tg.directory({ "tangram.py": await graph.get(0) });
        }')
    }
    let module = tg run $builder
    let output = tg check $module | complete
    failure $output
    assert ($output.stderr | str contains 'left.tg.py:1:') $output.stderr
    assert ($output.stderr | str contains 'left.tg.py:3:') $output.stderr
    failure (tg run $module | complete)
}
