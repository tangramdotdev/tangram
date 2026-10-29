use ../../lib/test.nu *
use ../../lib/lsp.nu

let local = server spawn
let source = "import { unused, value } from './helper.tg.ts';\nexport { value };\n"
let missing = "interface Shape { value: number; }\nexport class Box implements Shape {}\n"
let path = artifact {
    'main.tg.ts': $source
    'helper.tg.ts': 'export const unused = 0; export const value = 42;'
    'missing.tg.ts': $missing
}
let uri = lsp uri ($path | path join main.tg.ts)
let missing_uri = lsp uri ($path | path join missing.tg.ts)
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp did_open $uri $source)
    (lsp did_open $missing_uri $missing)
    (lsp request 10 'textDocument/codeAction' {
        textDocument: {uri: $uri}
        range: {start: {line: 0, character: 0}, end: {line: 1, character: 0}}
        context: {diagnostics: [], only: ['source.organizeImports']}
    })
    (lsp request 11 'textDocument/codeAction' {
        textDocument: {uri: $missing_uri}
        range: {start: {line: 1, character: 13}, end: {line: 1, character: 16}}
        context: {diagnostics: [], only: ['quickfix']}
    })
    (lsp request 12 'textDocument/codeAction' {
        textDocument: {uri: $uri}
        range: {start: {line: 0, character: 0}, end: {line: 1, character: 0}}
        context: {diagnostics: [], only: ['refactor']}
    })
]
let organized = lsp result $responses 10
assert equal ($organized | length) 1
assert equal $organized.0.kind 'source.organizeImports'
let edits = $organized.0.edit.changes | values | flatten
assert ($edits.0.newText | str contains value)
assert not ($edits.0.newText | str contains unused)
let fixes = lsp result $responses 11
assert (($fixes | length) > 0)
assert ($fixes | to nuon | str contains value)
assert equal (lsp result $responses 12) null
