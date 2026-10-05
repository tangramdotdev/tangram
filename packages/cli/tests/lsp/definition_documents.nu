use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let path = artifact {
    'tangram.ts': 'export {};'
    'main.tg.ts': "import { greet } from './helper.tg.ts';\nconst value = greet();"
    'helper.tg.ts': 'export function greet() { return 42; }'
}
let uri = lsp uri ($path | path join main.tg.ts)
let helper = $path | path join helper.tg.ts
let helper_uri = lsp uri $helper
mut session = lsp start
$session = lsp send_all $session [
    (lsp initialize 1)
    (lsp initialized)
    (lsp did_open $uri (open --raw ($path | path join main.tg.ts)))
    (lsp did_open $helper_uri "\n\nexport function greet() { return 43; }")
    (lsp definition 10 $uri 1 16)
]
let response = lsp wait_result $session 10
$session = $response.session
assert equal $response.result.0.range.start {line: 2, character: 16}

# Closing an edited dependency restores its disk contents in the retained checker.
$session = lsp send_all $session [
    (lsp notification 'textDocument/didClose' {textDocument: {uri: $helper_uri}})
    (lsp definition 11 $uri 1 16)
]
let response = lsp wait_result $session 11
$session = $response.session
assert equal $response.result.0.range.start {line: 0, character: 16}

# Closing a never-saved document must not prevent loading the file after it is created.
let new_path = $path | path join new.tg.ts
let new_uri = lsp uri $new_path
$session = lsp send_all $session [
    (lsp did_open $new_uri "function other() { return 42; }\nother();")
    (lsp definition 12 $new_uri 1 2)
]
let response = lsp wait_result $session 12
$session = $response.session
assert equal $response.result.0.range.start {line: 0, character: 9}
$session = lsp send_all $session [
    (lsp notification 'textDocument/didClose' {textDocument: {uri: $new_uri}})
    (lsp definition 13 $uri 1 16)
]
let response = lsp wait_result $session 13
$session = $response.session
assert equal $response.result.0.range.start {line: 0, character: 16}
let text = "\n\nexport function other() { return 43; }\nother();"
$text | save $new_path
$session = lsp send_all $session [
    (lsp notification 'textDocument/didChange' {
        textDocument: {uri: $uri, version: 2}
        contentChanges: [{text: "import { other } from './new.tg.ts';\nconst value = other();"}]
    })
    (lsp definition 14 $uri 1 16)
]
let response = lsp wait_result $session 14
$session = $response.session
assert equal $response.result.0.range.start {line: 2, character: 16}
lsp stop $session
