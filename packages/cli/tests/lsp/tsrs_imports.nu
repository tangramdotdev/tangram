use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let source = 'secret;'
let helper = 'export const secret = 42;'
let path = artifact {'main.tg.ts': $source, 'helper.tg.ts': $helper}
let uri = lsp uri ($path | path join main.tg.ts)
let helper_uri = lsp uri ($path | path join helper.tg.ts)
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp did_open $helper_uri $helper)
    (lsp did_open $uri $source)
    (lsp request 10 'textDocument/completion' {textDocument: {uri: $uri}, position: {line: 0, character: 3}})
    (lsp request 11 'textDocument/codeAction' {
        textDocument: {uri: $uri}
        range: {start: {line: 0, character: 0}, end: {line: 0, character: 6}}
        context: {diagnostics: [], only: ['quickfix']}
    })
]
# Generated module specifiers need a Tangram-aware rewrite before auto-imports can be offered.
assert ('secret' not-in (lsp result $responses 10 | default [] | get -o label | default []))
assert equal (lsp result $responses 11) null
