use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let dependency = artifact {'tangram.ts': "/** Greet a tagged friend. */\nexport function greet(name: string): string {\n    return name;\n}\n"}
tg tag -p greetings/1.0.0 $dependency
rm --recursive $dependency
let source = "import { greet } from 'greetings/^1';\nexport const value = greet(42);\n"
let other = "welcome = 42\n"
let path = artifact {'main.tg.ts': $source, 'other.tg.py': $other}
tg checkin $path | ignore
let uri = lsp uri ($path | path join main.tg.ts)
let py_uri = lsp uri ($path | path join other.tg.py)
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp did_open $uri $source)
    (lsp notification 'textDocument/didOpen' {textDocument: {uri: $py_uri, languageId: 'python', version: 1, text: $other}})
    (lsp hover 10 $uri 1 23)
    (lsp request 11 'textDocument/signatureHelp' {textDocument: {uri: $uri}, position: {line: 1, character: 28}})
    (lsp diagnostics 12 $uri)
    (lsp request 13 'textDocument/documentLink' {textDocument: {uri: $uri}})
    (lsp request 14 'workspace/symbol' {query: 'welc'})
    (lsp request 15 'workspace/symbol' {query: 'value'})
    (lsp definition 16 $uri 1 23)
]
assert ((lsp result $responses 10).contents.value | str contains 'name: string')
assert ((lsp result $responses 11).signatures.0.documentation | to nuon | str contains 'Greet a tagged friend')
assert ((lsp result $responses 11).signatures.0.label | str contains 'name: string')
let diagnostic = (lsp result $responses 12).items | where range.start.line == 1 | get 0
assert ($diagnostic.message | str contains 'string')
assert equal $diagnostic.range {start: {line: 1, character: 27}, end: {line: 1, character: 29}}
assert equal (lsp result $responses 14).0.location.uri $py_uri
assert equal ((lsp result $responses 15) | where location.uri == $uri | length) 1
let target = (lsp result $responses 16).0
assert equal $target.range {start: {line: 1, character: 16}, end: {line: 1, character: 21}}
assert equal (lsp result $responses 13).0.target $target.uri
assert (open --raw (lsp path_for_uri $target.uri) | str contains 'Greet a tagged friend')
