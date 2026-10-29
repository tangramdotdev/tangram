use ../../util/test.nu *
use ../../util/lsp.nu

let local = server spawn
let dependency = artifact {
    'tangram.ts': '// The source positions differ from the Python bindings.

export function greet() { return 42; }
export const other = () => 43;'
}
let source = $'# /// script
# [tool.tangram.dependencies.dep]
# specifier = "($dependency)"
# ///
from dep import greet, other
value = greet()
value = other()
'
let path = artifact {'main.tg.py': $source}
let uri = lsp uri ($path | path join main.tg.py)
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp definition 10 $uri 5 10)
    (lsp definition 11 $uri 6 10)
    (lsp request 12 textDocument/prepareRename {textDocument: {uri: $uri}, position: {line: 5, character: 10}})
    (lsp rename 13 $uri 5 10 welcome)
]

# The definitions are the exports' names in the original TypeScript module.
let dependency_uri = lsp uri ($dependency | path join tangram.ts)
assert equal (lsp result $responses 10) [{
    uri: $dependency_uri
    range: {start: {line: 2, character: 16}, end: {line: 2, character: 21}}
}]
assert equal (lsp result $responses 11) [{
    uri: $dependency_uri
    range: {start: {line: 3, character: 13}, end: {line: 3, character: 18}}
}]
assert equal (lsp result $responses 12) null
assert equal (lsp result $responses 13) null
