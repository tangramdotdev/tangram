use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let source = "class Box:\n    def size(self) -> int:\n        return 1\n\ndef greet(name: str) -> str:\n    \"\"\"Greet a friend.\"\"\"\n    return name\n\nanswer = greet(\"Ada\")\nbox = Box()\nbox.size()\n"
let path = artifact {'main.tg.py': $source}
let uri = lsp uri ($path | path join main.tg.py)
let document = {textDocument: {uri: $uri}}
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp notification 'textDocument/didOpen' {textDocument: {uri: $uri, languageId: 'python', version: 1, text: $source}})
    (lsp hover 10 $uri 8 10)
    (lsp request 11 'textDocument/signatureHelp' ($document | merge {position: {line: 8, character: 17}}))
    (lsp request 12 'textDocument/declaration' ($document | merge {position: {line: 8, character: 10}}))
    (lsp request 13 'textDocument/typeDefinition' ($document | merge {position: {line: 10, character: 1}}))
    (lsp request 14 'textDocument/documentSymbol' $document)
    (lsp request 15 'textDocument/foldingRange' $document)
    (lsp request 16 'textDocument/selectionRange' ($document | merge {positions: [{line: 8, character: 10}]}))
    (lsp request 17 'textDocument/documentHighlight' ($document | merge {position: {line: 8, character: 10}}))
    (lsp request 18 'textDocument/inlayHint' ($document | merge {range: {start: {line: 0, character: 0}, end: {line: 11, character: 0}}}))
    (lsp request 19 'textDocument/semanticTokens/full' $document)
    (lsp request 20 'textDocument/completion' ($document | merge {position: {line: 10, character: 8}}))
    (lsp request 21 'textDocument/documentLink' $document)
    (lsp diagnostics 22 $uri)
    (lsp request 23 'workspace/symbol' {query: 'greet'})
    (lsp request 24 'textDocument/implementation' ($document | merge {position: {line: 1, character: 9}}))
]
assert ((lsp result $responses 10).contents.value | str contains 'str')
assert ((lsp result $responses 10).contents.value | str contains 'Greet a friend')
assert ((lsp result $responses 11).signatures.0.label | str contains 'name: str')
assert equal (lsp result $responses 12).0.range {start: {line: 4, character: 4}, end: {line: 4, character: 9}}
assert equal (lsp result $responses 13).0.range.start {line: 0, character: 6}
let symbols = lsp result $responses 14
assert equal ($symbols | where name == 'greet' | get 0.selectionRange) {start: {line: 4, character: 4}, end: {line: 4, character: 9}}
assert equal ($symbols | where name == 'Box' | get 0.children.0.name) 'size'
assert (((lsp result $responses 15) | length) > 0)
assert equal (lsp result $responses 16).0.range {start: {line: 8, character: 9}, end: {line: 8, character: 14}}
assert equal ((lsp result $responses 17) | length) 2
assert (((lsp result $responses 18) | length) > 0)
assert (((lsp result $responses 19).data | length) > 0)
assert ('size' in (lsp result $responses 20 | get items.label))
assert equal (lsp result $responses 21) null
assert equal (lsp result $responses 22).items []
assert equal (lsp result $responses 23).0.location.uri $uri
assert equal (lsp result $responses 24).0.range.start {line: 1, character: 8}
