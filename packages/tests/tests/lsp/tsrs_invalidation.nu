use ../../lib/test.nu *
use ../../lib/lsp.nu

let local = server spawn
let source = "import { value } from './helper.tg.ts';\nexport const expected: number = value;\n"
let path = artifact {'main.tg.ts': $source, 'helper.tg.ts': 'export const value = 1;'}
let uri = lsp uri ($path | path join main.tg.ts)
mut session = lsp start
$session = lsp send_all $session [(lsp initialize 1) (lsp initialized) (lsp did_open $uri $source) (lsp diagnostics 10 $uri)]
let response = lsp wait_result $session 10
$session = $response.session
assert equal $response.result.items []

# Closed dependency changes must invalidate their cached source and dependent checker state.
'export const value = "changed";' | save --force ($path | path join helper.tg.ts)
$session = lsp send $session (lsp diagnostics 11 $uri)
let response = lsp wait_result $session 11
$session = $response.session
assert equal ($response.result.items | where severity == 1 | length) 1
'export const value = 2;' | save --force ($path | path join helper.tg.ts)
$session = lsp send $session (lsp diagnostics 12 $uri)
let response = lsp wait_result $session 12
$session = $response.session
assert equal $response.result.items []

# Deleted and recreated dependencies must also refresh cached resolutions.
rm ($path | path join helper.tg.ts)
$session = lsp send $session (lsp diagnostics 20 $uri)
let response = lsp wait_result $session 20
$session = $response.session
assert equal ($response.result.items | where severity == 1 | length) 1
'export const value = 3;' | save --force ($path | path join helper.tg.ts)
$session = lsp send $session (lsp diagnostics 21 $uri)
let response = lsp wait_result $session 21
$session = $response.session
assert equal $response.result.items []

# Missing imports must resolve after the dependency is created, without editing the importer again.
let changed = "import { value } from './created.tg.ts';\nexport const expected: number = value;\n"
$session = lsp send_all $session [
    (lsp notification 'textDocument/didChange' {textDocument: {uri: $uri, version: 2}, contentChanges: [{text: $changed}]})
    (lsp diagnostics 13 $uri)
]
let response = lsp wait_result $session 13
$session = $response.session
assert equal ($response.result.items | where severity == 1 | length) 1
'export const value = 99;' | save --force ($path | path join created.tg.ts)
$session = lsp send_all $session [(lsp diagnostics 14 $uri) (lsp definition 15 $uri 1 33)]
let response = lsp wait_result $session 14
$session = $response.session
assert equal $response.result.items []
let response = lsp wait_result $session 15
$session = $response.session
assert equal $response.result.0.uri (lsp uri ($path | path join created.tg.ts))
assert equal $response.result.0.range.start {line: 0, character: 13}
lsp stop $session
