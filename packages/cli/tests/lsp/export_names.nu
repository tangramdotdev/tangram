use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let path = artifact {'main.tg.ts': ''}
let uri = lsp uri ($path | path join main.tg.ts)
let source = '"🙂"; export const assert = () => 42;'

for encoding in ['utf-8', 'utf-16'] {
    let responses = lsp exchange [
        (lsp request 1 'initialize' {capabilities: {general: {positionEncodings: [$encoding]}}})
        (lsp initialized)
        (lsp did_open $uri $source)
        (lsp diagnostics 10 $uri)
    ]
    let diagnostic = (lsp result $responses 10).items | where message == 'python names the export assert as assert_' | get 0
    let start = if $encoding == 'utf-8' { 21 } else { 19 }
    assert equal $diagnostic.range {start: {line: 0, character: $start}, end: {line: 0, character: ($start + 6)}}
    assert equal $diagnostic.severity 2
}
