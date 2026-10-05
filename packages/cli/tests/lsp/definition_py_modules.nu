use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn

# Import-name navigation uses the same parent and member resolution as inference.
for initializer in [null 'value = 1'] {
    mut namespace = {'helper.tg.py': 'def greet(): return 42'}
    if $initializer != null {
        $namespace = $namespace | insert tangram.py $initializer
    }
    let source = "from .namespace import helper as alias\nvalue = alias.greet()\n"
    let path = artifact {'tangram.py': $source, namespace: $namespace}
    let uri = lsp uri ($path | path join tangram.py)
    let target = lsp uri ($path | path join namespace/helper.tg.py)
    let responses = lsp exchange [
        (lsp initialize 1)
        (lsp initialized)
        (lsp definition 10 $uri 0 25)
        (lsp definition 11 $uri 1 10)
    ]
    let expected = [{uri: $target, range: {start: {line: 0, character: 0}, end: {line: 0, character: 0}}}]
    assert equal (lsp result $responses 10) $expected
    assert equal (lsp result $responses 11) $expected
}

# Existing module exports retain their identity when the host declines to override them.
let path = artifact {
    'tangram.py': 'from . import other as helper'
    'other.tg.py': 'value = 42'
    'left.tg.py': "from . import helper\nvalue = helper.value\n"
}
let uri = lsp uri ($path | path join left.tg.py)
let target = lsp uri ($path | path join other.tg.py)
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp definition 10 $uri 0 16)
    (lsp definition 11 $uri 1 10)
]
let expected = [{uri: $target, range: {start: {line: 0, character: 0}, end: {line: 0, character: 0}}}]
assert equal (lsp result $responses 10) $expected
assert equal (lsp result $responses 11) $expected

# A checked-in member edge overrides a module re-export on the package initializer.
for missing in [false true] {
    let target = if $missing { 'null' } else { 'member' }
    let builder = artifact {
        'tangram.ts': ('export default async function () {
            const member = await tg.file("value = 42").module("py");
            const other = await tg.file("value = 43").module("py");
            const graph = await tg.graph({ nodes: [
                { kind: "file", module: "py", contents: "from . import helper\nfrom . import left", dependencies: { "./tangram.py": 0, "./helper.tg.py": other, "./left.tg.py": 1 } },
                { kind: "file", module: "py", contents: "from . import helper\nvalue = helper.value", dependencies: { "./left.tg.py": 1, "./tangram.py": 0, "./helper.tg.py": ' + $target + ' } },
            ] });
            return tg.directory({ "tangram.py": await graph.get(0) });
        }')
    }
    let dependency = tg run $builder
    let source = $'# /// script
# [tool.tangram.imports.dep]
# specifier = "($dependency)"
# ///
from dep import left
value = left
'
    let path = artifact {'main.tg.py': $source}
    let uri = lsp uri ($path | path join main.tg.py)
    mut session = lsp start
    $session = lsp send_all $session [(lsp initialize 1) (lsp initialized) (lsp definition 10 $uri 4 17)]
    let response = lsp wait_result $session 10
    $session = $response.session
    let left = $response.result.0.uri
    $session = lsp send_all $session [(lsp definition 11 $left 0 16) (lsp definition 12 $left 1 10)]
    for id in [11 12] {
        let response = lsp wait_result $session $id
        $session = $response.session
        if $missing {
            # An unresolved use selects its local binding; the import itself has no target.
            if $id == 12 {
                assert equal $response.result [{uri: $left, range: {start: {line: 0, character: 14}, end: {line: 0, character: 20}}}]
            } else {
                assert equal $response.result null
            }
        } else {
            assert equal ($response.result | length) 1
            let file = lsp path_for_uri $response.result.0.uri
            assert equal (open --raw $file) 'value = 42'
        }
    }
    lsp stop $session
}
