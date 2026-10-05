use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let dependency = artifact {
    'tangram.ts': '// The source positions differ from the generated Python wrapper.

export function greet() { return 42; }
export const other = () => 43;'
}
let source = $'# /// script
# [tool.tangram.imports.dep]
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
]
let locations = lsp result $responses 10
assert equal $locations [{uri: (lsp uri ($dependency | path join tangram.ts)), range: {start: {line: 2, character: 16}, end: {line: 2, character: 21}}}]
let locations = lsp result $responses 11
assert equal $locations.0.range {start: {line: 3, character: 13}, end: {line: 3, character: 18}}
