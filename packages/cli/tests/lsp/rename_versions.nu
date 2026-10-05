use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let source = "import { greet } from './helper.tg.ts';\nconst value = greet();"
let helper = 'export function greet() { return 42; }'
let path = artifact {'tangram.ts': $source, 'helper.tg.ts': $helper}
let uri = lsp uri ($path | path join tangram.ts)
let target = lsp uri ($path | path join helper.tg.ts)
mut session = lsp start
$session = lsp send_all $session [
    (lsp initialize 1)
    (lsp initialized)
    (lsp did_open $uri $source 17)
    (lsp did_open $target $helper 42)
    (lsp rename 10 $uri 1 16 hello)
]
let response = lsp wait_result $session 10
$session = $response.session
let edits = $response.result.documentChanges
assert equal ($edits | length) 2
assert equal ($edits | where textDocument.uri == $uri | first | get textDocument.version) 17
assert equal ($edits | where textDocument.uri == $target | first | get textDocument.version) 42

# Closed documents use null, while reopened documents use the new editor version.
$session = lsp send_all $session [
    (lsp notification 'textDocument/didClose' {textDocument: {uri: $target}})
    (lsp notification 'textDocument/didClose' {textDocument: {uri: $uri}})
    (lsp did_open $uri $source 0)
    (lsp rename 11 $uri 1 16 hello)
]
let response = lsp wait_result $session 11
$session = $response.session
let edits = $response.result.documentChanges
assert equal ($edits | length) 2
assert equal ($edits | where textDocument.uri == $uri | first | get textDocument.version) 0
assert equal ($edits | where textDocument.uri == $target | first | get textDocument.version) null
lsp stop $session
