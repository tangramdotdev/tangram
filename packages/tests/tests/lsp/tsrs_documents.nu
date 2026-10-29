use ../../util/test.nu *
use ../../util/lsp.nu

let local = server spawn
let path = artifact {
    tangram.ts: 'export {};'
    main.tg.ts: "import { value } from './helper.tg.ts';\nexport const result: number = value;"
    helper.tg.ts: 'export const value = 42;'
}
let uri = lsp uri ($path | path join main.tg.ts)
let helper_uri = lsp uri ($path | path join helper.tg.ts)
mut session = lsp start
$session = lsp send_all $session [
    (lsp initialize 1)
    (lsp initialized)
    (lsp did_open $uri (open --raw ($path | path join main.tg.ts)))
    (lsp diagnostics 10 $uri)
]
let response = lsp wait_result $session 10
$session = $response.session
assert equal $response.result.items []

# Unsaved dependency contents must affect both checking and navigation.
$session = lsp send_all $session [
    (lsp did_open $helper_uri "\n\nexport const value = 'wrong';")
    (lsp diagnostics 11 $uri)
    (lsp definition 12 $uri 1 30)
]
let response = lsp wait_result $session 11
$session = $response.session
assert equal ($response.result.items | length) 1
let response = lsp wait_result $session 12
$session = $response.session
assert equal $response.result.0.range.start {line: 2, character: 13}

# Closing the dependency restores disk contents and clears the error.
$session = lsp send_all $session [
    (lsp notification 'textDocument/didClose' {textDocument: {uri: $helper_uri}})
    (lsp diagnostics 13 $uri)
    (lsp definition 14 $uri 1 30)
]
let response = lsp wait_result $session 13
$session = $response.session
assert equal $response.result.items []
let response = lsp wait_result $session 14
$session = $response.session
assert equal $response.result.0.range.start {line: 0, character: 13}
lsp stop $session
