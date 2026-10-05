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
    (lsp request 12 textDocument/prepareRename {textDocument: {uri: $uri}, position: {line: 5, character: 10}})
    (lsp rename 13 $uri 5 10 welcome)
]
let location = (lsp result $responses 10).0
let generated = lsp path_for_uri $location.uri
assert ($generated | str contains '/generated/')
assert ($generated | str ends-with '.tg.py')
let text = open --raw $generated
let line = $text | lines | get $location.range.start.line
assert equal ($line | str substring $location.range.start.character..<$location.range.end.character) 'greet'
assert ($line | str contains 'Command.function')
let other = (lsp result $responses 11).0
assert equal $other.uri $location.uri
assert equal ($text | lines | get $other.range.start.line | str substring $other.range.start.character..<$other.range.end.character) 'other'
assert equal (lsp result $responses 12) null
assert equal (lsp result $responses 13) null

# Opening the generated module must not allow edits through rename.
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp did_open $location.uri $text)
    (lsp rename 10 $location.uri $location.range.start.line $location.range.start.character welcome)
]
assert equal (lsp result $responses 10) null
