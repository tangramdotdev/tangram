use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let path = artifact {'main.tg.py': ''}
let uri = lsp uri ($path | path join main.tg.py)
let source = "\"🙂\"; value: int = \"wrong\"\n"
mut session = lsp start
$session = lsp send_all $session [
    (lsp initialize 1)
    (lsp initialized)
    (lsp notification 'textDocument/didOpen' {textDocument: {uri: $uri, languageId: 'python', version: 1, text: $source}})
    (lsp diagnostics 10 $uri)
]
let response = lsp wait_result $session 10
$session = $response.session
let diagnostic = $response.result.items.0
assert ($diagnostic.message | str contains 'int')
assert equal $diagnostic.range {start: {line: 0, character: 19}, end: {line: 0, character: 26}}
$session = lsp send_all $session [
    (lsp request 11 'textDocument/codeAction' {textDocument: {uri: $uri}, range: $diagnostic.range, context: {diagnostics: [$diagnostic], only: ['quickfix']}})
    (lsp request 12 'textDocument/codeAction' {textDocument: {uri: $uri}, range: $diagnostic.range, context: {diagnostics: [$diagnostic], only: ['source.organizeImports']}})
]
let response = lsp wait_result $session 11
$session = $response.session
let action = $response.result.0
assert ($action.title | str contains 'Ignore')
let edit = $action.edit.changes | values | get 0.0
assert ($edit.newText | str contains 'ty: ignore')
assert equal $edit.range.start.line 0
let response = lsp wait_result $session 12
$session = $response.session
assert equal $response.result null
$session = lsp send_all $session [
    (lsp notification 'textDocument/didChange' {textDocument: {uri: $uri, version: 2}, contentChanges: [{text: "value: int = 42\n"}]})
    (lsp diagnostics 13 $uri)
]
let response = lsp wait_result $session 13
$session = $response.session
assert equal $response.result.items []
lsp stop $session

# The diagnostics use the encoding negotiated by the same shared LSP server.
let responses = lsp exchange [
    (lsp request 1 'initialize' {capabilities: {general: {positionEncodings: ['utf-8']}}})
    (lsp initialized)
    (lsp notification 'textDocument/didOpen' {textDocument: {uri: $uri, languageId: 'python', version: 1, text: $source}})
    (lsp diagnostics 20 $uri)
]
assert equal (lsp result $responses 20).items.0.range {start: {line: 0, character: 21}, end: {line: 0, character: 28}}
