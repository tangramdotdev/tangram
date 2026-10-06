use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let path = artifact {'main.tg.ts': ''}
let uri = lsp uri ($path | path join main.tg.ts)
let source = "\"🙂\"; const value: number = \"wrong\";\nMath.florr(1);\nvalue;\n"
mut session = lsp start
$session = lsp send_all $session [
    (lsp initialize 1)
    (lsp initialized)
    (lsp did_open $uri $source)
    (lsp diagnostics 10 $uri)
]
let response = lsp wait_result $session 10
$session = $response.session
let diagnostic = $response.result.items | where range.start.line == 0 | get 0
assert ($diagnostic.message | str contains 'number')
assert equal $diagnostic.range {start: {line: 0, character: 12}, end: {line: 0, character: 17}}
let diagnostic = $response.result.items | where range.start.line == 1 | get 0
$session = lsp send_all $session [
    (lsp request 11 'textDocument/codeAction' {textDocument: {uri: $uri}, range: $diagnostic.range, context: {diagnostics: [$diagnostic], only: ['quickfix']}})
    (lsp request 12 'textDocument/codeAction' {textDocument: {uri: $uri}, range: $diagnostic.range, context: {diagnostics: [$diagnostic], only: ['source.organizeImports']}})
]
let response = lsp wait_result $session 11
$session = $response.session
let action = $response.result | where { |action| $action.title | str contains 'floor' } | get 0
assert equal $action.kind quickfix
let edit = $action.edit.changes | values | get 0.0
assert equal $edit.newText floor
assert equal $edit.range {start: {line: 1, character: 5}, end: {line: 1, character: 10}}
let response = lsp wait_result $session 12
$session = $response.session
assert equal $response.result null
$session = lsp send_all $session [
    (lsp notification 'textDocument/didChange' {textDocument: {uri: $uri, version: 2}, contentChanges: [{text: "const value: number = 42;\nMath.floor(1);\nvalue;\n"}]})
    (lsp diagnostics 13 $uri)
]
let response = lsp wait_result $session 13
$session = $response.session
assert equal $response.result.items []
lsp stop $session
