use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let path = artifact {'main.tg.py': "def disk(): pass\n\ndisk()\n"}
let module = $path | path join main.tg.py
let uri = lsp uri $module
mut session = lsp start
$session = lsp send_all $session [(lsp initialize 1) (lsp initialized)]
let response = lsp wait_result $session 1
$session = $response.session

# Queries use open text, including UTF-16 positions after a non-BMP character.
let source = "def café(): pass\n\n\"🙂\"; café()\n"
$session = lsp send_all $session [
    (lsp notification 'textDocument/didOpen' {textDocument: {uri: $uri, languageId: 'python', version: 1, text: $source}})
    (lsp definition 10 $uri 2 7)
]
let response = lsp wait_result $session 10
$session = $response.session
assert equal $response.result [{uri: $uri, range: {start: {line: 0, character: 4}, end: {line: 0, character: 8}}}]

# An incremental edit updates both the queried and returned locations.
$session = lsp send_all $session [
    (lsp notification 'textDocument/didChange' {
        textDocument: {uri: $uri, version: 2}
        contentChanges: [{range: {start: {line: 0, character: 0}, end: {line: 0, character: 0}}, text: "\n\n"}]
    })
    (lsp definition 11 $uri 4 7)
]
let response = lsp wait_result $session 11
$session = $response.session
assert equal $response.result.0.range.start {line: 2, character: 4}

# Closing the document restores disk contents without restarting the language server.
$session = lsp send_all $session [
    (lsp notification 'textDocument/didClose' {textDocument: {uri: $uri}})
    (lsp definition 12 $uri 2 1)
]
let response = lsp wait_result $session 12
$session = $response.session
assert equal $response.result.0.range {start: {line: 0, character: 4}, end: {line: 0, character: 8}}

# Changes to a closed file are visible to subsequent queries.
"\n\ndef changed(): pass\nchanged()\n" | save --force $module
$session = lsp send $session (lsp definition 13 $uri 3 2)
let response = lsp wait_result $session 13
$session = $response.session
assert equal $response.result.0.range {start: {line: 2, character: 4}, end: {line: 2, character: 11}}
lsp stop $session

# The same conversion works with a negotiated UTF-8 position encoding.
let responses = lsp exchange [
    (lsp request 1 'initialize' {capabilities: {general: {positionEncodings: ['utf-8']}}})
    (lsp initialized)
    (lsp notification 'textDocument/didOpen' {textDocument: {uri: $uri, languageId: 'python', version: 3, text: $source}})
    (lsp definition 20 $uri 2 9)
]
let locations = lsp result $responses 20
assert equal $locations.0.range {start: {line: 0, character: 4}, end: {line: 0, character: 9}}
