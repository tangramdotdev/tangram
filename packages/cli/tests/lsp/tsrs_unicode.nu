use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let source = 'const face = "😀"; export const item: number = "wrong"; item;'
let path = artifact {main.tg.ts: $source}
let uri = lsp uri ($path | path join main.tg.ts)

for encoding in [
    {name: 'utf-8', declaration: 34, reference: 58}
    {name: 'utf-16', declaration: 32, reference: 56}
] {
    mut session = lsp start
    $session = lsp send_all $session [
        (lsp request 1 'initialize' {processId: null, rootUri: null, capabilities: {general: {positionEncodings: [$encoding.name]}}})
        (lsp initialized)
        (lsp did_open $uri $source)
        (lsp diagnostics 10 $uri)
        (lsp definition 11 $uri 0 $encoding.reference)
        (lsp hover 12 $uri 0 $encoding.reference)
    ]
    let response = lsp wait_result $session 1
    $session = $response.session
    assert equal $response.result.capabilities.positionEncoding $encoding.name
    let response = lsp wait_result $session 10
    $session = $response.session
    let diagnostics = $response.result.items | where severity == 1
    assert equal ($diagnostics | length) 1
    assert equal $diagnostics.0.range.start {line: 0, character: $encoding.declaration}
    assert ($diagnostics.0.message | str contains "Type 'string' is not assignable to type 'number'")
    let hints = $response.result.items | where severity == 4
    assert equal ($hints | length) 1
    assert equal $hints.0.range {start: {line: 0, character: 6}, end: {line: 0, character: 10}}
    assert ($hints.0.message | str contains "'face' is declared but its value is never read")
    let response = lsp wait_result $session 11
    $session = $response.session
    assert equal $response.result.0.range.start {line: 0, character: $encoding.declaration}
    let response = lsp wait_result $session 12
    $session = $response.session
    assert ($response.result.contents.value | str contains 'number')

    $session = lsp send_all $session [
        (lsp notification 'textDocument/didChange' {textDocument: {uri: $uri, version: 2}, contentChanges: [{text: ($source | str replace '"wrong"' '42')}]})
        (lsp diagnostics 13 $uri)
    ]
    let response = lsp wait_result $session 13
    $session = $response.session
    assert equal ($response.result.items | where severity == 1) []
    assert equal ($response.result.items | where severity == 4) $hints
    lsp stop $session
}
