use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn --config {compiler: {check_backend: tsrs, typescript_backend: tsrs}}
let dependency = artifact {
    tangram.ts: 'export default 0;'
    number.tg.ts: 'export const value = 42;'
    string.tg.ts: 'export const value = "hello";'
}
tg tag -p tsrs-editor/1.0.0 $dependency
rm --recursive $dependency
let source = "import { value as number } from 'tsrs-editor/^1' with { get: 'number.tg.ts' };\nimport { value as text } from 'tsrs-editor/^1' with { get: 'string.tg.ts' };\nnumber;\ntext;\nexport { value } from 'tsrs-editor/^1' with { get: 'number.tg.ts' };\nconst dynamic = import('tsrs-editor/^1', { with: { get: 'string.tg.ts' } });\n"
let path = artifact {'main.tg.ts': $source}
let uri = lsp uri ($path | path join main.tg.ts)
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp did_open $uri $source)
    (lsp hover 10 $uri 2 1)
    (lsp hover 11 $uri 3 1)
    (lsp definition 12 $uri 2 1)
    (lsp definition 13 $uri 3 1)
    (lsp document_link 14 $uri)
]
assert ((lsp result $responses 10).contents.value | str contains '42')
assert ((lsp result $responses 11).contents.value | str contains '"hello"')
let number_target = (lsp result $responses 12).0.uri
let string_target = (lsp result $responses 13).0.uri
assert ($number_target != $string_target)
let links = lsp result $responses 14
assert equal ($links | length) 4
assert equal $links.0.target $number_target
assert equal $links.1.target $string_target
assert equal $links.2.target $number_target
assert equal $links.3.target $string_target
assert equal $links.0.range {start: {line: 0, character: 33}, end: {line: 0, character: 47}}

# Attribute-only edits must update resolution inside the retained language service project.
mut session = lsp start
$session = lsp send_all $session [(lsp initialize 1) (lsp initialized) (lsp did_open $uri $source) (lsp hover 20 $uri 2 1)]
let response = lsp wait_result $session 20
$session = $response.session
assert ($response.result.contents.value | str contains '42')
let changed = $source | str replace --all 'number.tg.ts' 'string.tg.ts'
$session = lsp send_all $session [
    (lsp notification 'textDocument/didChange' {textDocument: {uri: $uri, version: 2}, contentChanges: [{text: $changed}]})
    (lsp hover 21 $uri 2 1)
    (lsp definition 22 $uri 2 1)
]
let response = lsp wait_result $session 21
$session = $response.session
assert ($response.result.contents.value | str contains '"hello"')
let response = lsp wait_result $session 22
$session = $response.session
assert equal $response.result.0.uri $string_target
lsp stop $session
