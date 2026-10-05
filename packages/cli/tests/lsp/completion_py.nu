use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let source = "def greet(name: str) -> str:\n    \"\"\"Greet a friend.\"\"\"\n    return name\n\ndef welcome() -> str:\n    return greet(\"Ada\")\n\ngre\n"
let path = artifact {'main.tg.py': $source}
let uri = lsp uri ($path | path join main.tg.py)
mut session = lsp start
$session = lsp send_all $session [
    (lsp initialize 1)
    (lsp initialized)
    (lsp notification 'textDocument/didOpen' {textDocument: {uri: $uri, languageId: 'python', version: 1, text: $source}})
    (lsp request 10 'textDocument/completion' {textDocument: {uri: $uri}, position: {line: 7, character: 3}})
]
let response = lsp wait_result $session 10
$session = $response.session
let item = $response.result.items | where label == 'greet' | get 0
$session = lsp send_all $session [
    (lsp request 11 'completionItem/resolve' $item)
    (lsp request 12 'textDocument/prepareCallHierarchy' {textDocument: {uri: $uri}, position: {line: 4, character: 6}})
]
let response = lsp wait_result $session 11
$session = $response.session
assert ($response.result.detail | str contains 'str')
assert ($response.result.documentation | to nuon | str contains 'Greet a friend')
let response = lsp wait_result $session 12
$session = $response.session
$session = lsp send $session (lsp request 13 'callHierarchy/outgoingCalls' {item: $response.result.0})
let response = lsp wait_result $session 13
$session = $response.session
assert equal $response.result.0.to.name greet
assert equal $response.result.0.fromRanges.0.start {line: 5, character: 11}
lsp stop $session
