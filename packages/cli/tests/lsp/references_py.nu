use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let source = "def greet(name: str) -> str:\n    return name\n\ngreet(\"Ada\")\n"
let consumer = "from .library import greet\n\ngreet(\"Grace\")\n"
let path = artifact {'tangram.py': '', 'main.tg.py': $consumer, 'library.tg.py': $source}
let uri = lsp uri ($path | path join library.tg.py)
let main_uri = lsp uri ($path | path join main.tg.py)
mut session = lsp start
$session = lsp send_all $session [
    (lsp initialize 1)
    (lsp initialized)
    (lsp notification 'textDocument/didOpen' {textDocument: {uri: $uri, languageId: 'python', version: 3, text: $source}})
    (lsp notification 'textDocument/didOpen' {textDocument: {uri: $main_uri, languageId: 'python', version: 5, text: $consumer}})
    (lsp request 10 'textDocument/references' {textDocument: {uri: $uri}, position: {line: 0, character: 5}, context: {includeDeclaration: true}})
]
let response = lsp wait_result $session 10
$session = $response.session
assert equal ($response.result | length) 4
assert ($main_uri in $response.result.uri)
$session = lsp send_all $session [
    (lsp request 11 'textDocument/references' {textDocument: {uri: $uri}, position: {line: 0, character: 5}, context: {includeDeclaration: false}})
    (lsp request 12 'textDocument/prepareRename' {textDocument: {uri: $uri}, position: {line: 0, character: 5}})
    (lsp rename 13 $uri 0 5 welcome)
    (lsp request 14 'textDocument/prepareCallHierarchy' {textDocument: {uri: $uri}, position: {line: 0, character: 5}})
]
let response = lsp wait_result $session 11
$session = $response.session
assert equal ($response.result | length) 2
let response = lsp wait_result $session 12
$session = $response.session
assert equal $response.result.placeholder greet
let response = lsp wait_result $session 13
$session = $response.session
assert equal ($response.result.documentChanges | length) 2
assert equal ($response.result.documentChanges | where textDocument.uri == $main_uri | get 0.textDocument.version) 5
assert equal ($response.result.documentChanges | where textDocument.uri == $uri | get 0.textDocument.version) 3
assert equal ($response.result.documentChanges.edits | flatten | get newText | uniq) [welcome]
let response = lsp wait_result $session 14
$session = $response.session
let item = $response.result.0
$session = lsp send $session (lsp request 15 'callHierarchy/incomingCalls' {item: $item})
let response = lsp wait_result $session 15
$session = $response.session
assert ($main_uri in $response.result.from.uri)

# A newly opened file joins the same incremental project, without recreating ty.
let other_uri = lsp uri ($path | path join other.tg.py)
$session = lsp send_all $session [
    (lsp notification 'textDocument/didOpen' {textDocument: {uri: $other_uri, languageId: 'python', version: 1, text: $consumer}})
    (lsp request 16 'textDocument/references' {textDocument: {uri: $uri}, position: {line: 0, character: 5}, context: {includeDeclaration: true}})
]
let response = lsp wait_result $session 16
$session = $response.session
assert ($other_uri in $response.result.uri)
$session = lsp send_all $session [
    (lsp notification 'textDocument/didChange' {textDocument: {uri: $other_uri, version: 2}, contentChanges: [{text: "other = 1\n"}]})
    (lsp request 17 'textDocument/references' {textDocument: {uri: $uri}, position: {line: 0, character: 5}, context: {includeDeclaration: true}})
]
let response = lsp wait_result $session 17
$session = $response.session
assert ($other_uri not-in $response.result.uri)
# Closed, unreferenced documents leave the project symbol set.
$session = lsp send_all $session [
    (lsp request 18 'workspace/symbol' {query: 'other'})
]
let response = lsp wait_result $session 18
$session = $response.session
assert equal $response.result.0.location.uri $other_uri
$session = lsp send_all $session [
    (lsp notification 'textDocument/didClose' {textDocument: {uri: $other_uri}})
    (lsp request 19 'workspace/symbol' {query: 'other'})
]
let response = lsp wait_result $session 19
$session = $response.session
assert equal $response.result []
$session = lsp send_all $session [
    (lsp notification 'textDocument/didClose' {textDocument: {uri: $main_uri}})
    (lsp notification 'textDocument/didClose' {textDocument: {uri: $uri}})
    (lsp request 20 'workspace/symbol' {query: 'greet'})
]
let response = lsp wait_result $session 20
$session = $response.session
assert equal $response.result []
lsp stop $session
