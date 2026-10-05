use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let dependency = artifact {
    'tangram.py': 'from .helper import greet'
    'helper.tg.py': 'def greet(): return 42'
}
tg tag lsp-python $dependency
for specifier in [$dependency 'lsp-python'] {
    let source = $'# /// script
# [tool.tangram.imports.dep]
# specifier = "($specifier)"
# ///
from dep import greet
value = greet()
'
    let path = artifact {'main.tg.py': $source}
    let uri = lsp uri ($path | path join main.tg.py)
    let responses = lsp exchange [
        (lsp initialize 1)
        (lsp initialized)
        (lsp definition 10 $uri 5 10)
    ]
    let locations = lsp result $responses 10
    assert (($locations | length) > 0)
    let location = $locations.0
    assert equal $location.range {start: {line: 0, character: 4}, end: {line: 0, character: 9}}
    let file = lsp path_for_uri $location.uri
    assert (open --raw $file | str contains 'def greet')
    assert ($file | str ends-with '.tg.py')
}

# An unsaved dependency declaration remains available with an incomplete statement elsewhere.
let path = artifact {'main.tg.py': 'pass'}
let uri = lsp uri ($path | path join main.tg.py)
let source = $'# /// script
# [tool.tangram.imports.dep]
# specifier = "($dependency)"
# ///
from dep import greet
value = greet()
unfinished = 
'
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp notification 'textDocument/didOpen' {textDocument: {uri: $uri, languageId: 'python', version: 1, text: $source}})
    (lsp definition 10 $uri 5 10)
]
let locations = lsp result $responses 10
assert equal $locations.0.range.start {line: 0, character: 4}
