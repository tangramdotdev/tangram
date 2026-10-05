use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let dependency = artifact {'tangram.py': "def greet(name: str) -> str:\n    \"\"\"Greet a tagged friend.\"\"\"\n    return name\n"}
tg tag -p greetings/1.0.0 $dependency
rm --recursive $dependency
let source = '# /// script
# [tool.tangram.imports.friend]
# specifier = "greetings/^1"
# ///
from friend import greet
value = greet(42)
'
let path = artifact {'main.tg.py': $source, 'other.tg.ts': 'export const welcome = 42;'}
tg checkin $path | ignore
let uri = lsp uri ($path | path join main.tg.py)
let js_uri = lsp uri ($path | path join other.tg.ts)
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp notification 'textDocument/didOpen' {textDocument: {uri: $uri, languageId: 'python', version: 1, text: $source}})
    (lsp did_open $js_uri 'export const welcome = 42;')
    (lsp hover 10 $uri 5 10)
    (lsp request 11 'textDocument/signatureHelp' {textDocument: {uri: $uri}, position: {line: 5, character: 15}})
    (lsp diagnostics 12 $uri)
    (lsp request 13 'textDocument/prepareRename' {textDocument: {uri: $uri}, position: {line: 5, character: 10}})
    (lsp request 14 'workspace/symbol' {query: 'welc'})
    (lsp request 15 'workspace/symbol' {query: 'value'})
]
assert ((lsp result $responses 10).contents.value | str contains 'Greet a tagged friend')
assert ((lsp result $responses 11).signatures.0.label | str contains 'name: str')
assert ((lsp result $responses 12).items.0.message | str contains 'str')
assert equal (lsp result $responses 12).items.0.range.start.line 5
assert equal (lsp result $responses 13) null
assert equal (lsp result $responses 14).0.location.uri $js_uri
assert equal ((lsp result $responses 15) | where location.uri == $uri | length) 1
