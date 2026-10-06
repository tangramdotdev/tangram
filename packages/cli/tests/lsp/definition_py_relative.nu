use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let path = artifact {
    'tangram.py': "from .namespace.helper import greet\nvalue = greet()\n"
    namespace: {'helper.tg.py': 'def greet(): return 42'}
}
let uri = lsp uri ($path | path join tangram.py)
let helper = $path | path join namespace/helper.tg.py
let helper_uri = lsp uri $helper
mut session = lsp start
$session = lsp send_all $session [(lsp initialize 1) (lsp initialized)]
let response = lsp wait_result $session 1
$session = $response.session
$session = lsp send_all $session [
    (lsp notification 'textDocument/didOpen' {textDocument: {uri: $helper_uri, languageId: 'python', version: 1, text: "\n\ndef greet(): return 43\n"}})
    (lsp definition 10 $uri 1 10)
]
let response = lsp wait_result $session 10
$session = $response.session
assert equal $response.result [{uri: $helper_uri, range: {start: {line: 2, character: 4}, end: {line: 2, character: 9}}}]
$session = lsp send_all $session [
    (lsp notification 'textDocument/didClose' {textDocument: {uri: $helper_uri}})
    (lsp definition 11 $uri 1 10)
]
let response = lsp wait_result $session 11
$session = $response.session
assert equal $response.result.0.range.start {line: 0, character: 4}

# Relative imports can target a new editor buffer before its first save.
let new_uri = lsp uri ($path | path join new.tg.py)
$session = lsp send_all $session [
    (lsp notification 'textDocument/didOpen' {textDocument: {uri: $new_uri, languageId: 'python', version: 1, text: 'def greet(): return 44'}})
    (lsp notification 'textDocument/didOpen' {textDocument: {uri: $uri, languageId: 'python', version: 1, text: "from .new import greet\nvalue = greet()\n"}})
    (lsp definition 12 $uri 1 10)
]
let response = lsp wait_result $session 12
$session = $response.session
assert equal $response.result [{uri: $new_uri, range: {start: {line: 0, character: 4}, end: {line: 0, character: 9}}}]
# Closing a never-saved buffer and later creating the file updates the same cached module.
$session = lsp send_all $session [
    (lsp notification 'textDocument/didClose' {textDocument: {uri: $new_uri}})
    (lsp definition 13 $uri 1 10)
]
let response = lsp wait_result $session 13
$session = $response.session
assert ($response.result | default [] | all {|location| $location.uri != $new_uri})
"\n\ndef greet(): return 45\n" | save ($path | path join new.tg.py)
$session = lsp send $session (lsp definition 14 $uri 1 10)
let response = lsp wait_result $session 14
$session = $response.session
assert equal $response.result.0.range.start {line: 2, character: 4}
lsp stop $session
