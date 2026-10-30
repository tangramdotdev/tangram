use ../../util/test.nu *
use ../../util/lsp.nu

# The language server reports an invalid config or interface at the position of its error, and answers other requests for config documents with empty results.

let local = server spawn
let path = artifact {'env.tg.toml': ''}
let uri = lsp uri ($path | path join env.tg.toml)
let source = "runtime = \"./runtime\"\nimports = {}\n"
mut session = lsp start
$session = lsp send_all $session [
    (lsp initialize 1)
    (lsp initialized)
    (lsp notification 'textDocument/didOpen' {textDocument: {uri: $uri, languageId: 'toml', version: 1, text: $source}})
    (lsp diagnostics 10 $uri)
    (lsp hover 11 $uri 0 2)
]
let response = lsp wait_result $session 10
$session = $response.session
let diagnostic = $response.result.items.0
assert ($diagnostic.message | str contains 'unknown field') $diagnostic.message
assert equal $diagnostic.range.start.line 1
let response = lsp wait_result $session 11
$session = $response.session
assert equal $response.result null
$session = lsp send_all $session [
    (lsp notification 'textDocument/didChange' {textDocument: {uri: $uri, version: 2}, contentChanges: [{text: "runtime = \"./runtime\"\n"}]})
    (lsp diagnostics 12 $uri)
]
let response = lsp wait_result $session 12
$session = $response.session
assert equal $response.result.items []
lsp stop $session

# An invalid interface is reported at the position of its error.
let path = artifact {'env.tg.tgi': ''}
let uri = lsp uri ($path | path join env.tg.tgi)
let source = "{\n  \"exports\": 1\n}\n"
mut session = lsp start
$session = lsp send_all $session [
    (lsp initialize 1)
    (lsp initialized)
    (lsp notification 'textDocument/didOpen' {textDocument: {uri: $uri, languageId: 'json', version: 1, text: $source}})
    (lsp diagnostics 10 $uri)
]
let response = lsp wait_result $session 10
$session = $response.session
let diagnostic = $response.result.items.0
assert ($diagnostic.message | str contains 'invalid interface') $diagnostic.message
assert equal $diagnostic.range.start.line 1
lsp stop $session
