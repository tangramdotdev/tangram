use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let source = "export class Box {\n    size(): number {\n        return 1;\n    }\n}\n/** Greet a friend. */\nexport function greet(name: string): string {\n    return name;\n}\nconst answer = greet(\"Ada\");\nconst box = new Box();\nbox.size();\nanswer;\n"
let path = artifact {'main.tg.ts': $source}
let uri = lsp uri ($path | path join main.tg.ts)
let document = {textDocument: {uri: $uri}}
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp did_open $uri $source)
    (lsp hover 10 $uri 9 16)
    (lsp request 11 'textDocument/signatureHelp' ($document | merge {position: {line: 9, character: 24}}))
    (lsp request 12 'textDocument/declaration' ($document | merge {position: {line: 9, character: 16}}))
    (lsp request 13 'textDocument/typeDefinition' ($document | merge {position: {line: 11, character: 1}}))
    (lsp request 14 'textDocument/documentSymbol' $document)
    (lsp request 15 'textDocument/foldingRange' $document)
    (lsp request 16 'textDocument/selectionRange' ($document | merge {positions: [{line: 9, character: 16}]}))
    (lsp request 17 'textDocument/documentHighlight' ($document | merge {position: {line: 9, character: 16}}))
    (lsp request 18 'textDocument/inlayHint' ($document | merge {range: {start: {line: 0, character: 0}, end: {line: 13, character: 0}}}))
    (lsp request 19 'textDocument/semanticTokens/full' $document)
    (lsp request 20 'textDocument/completion' ($document | merge {position: {line: 11, character: 4}}))
    (lsp request 21 'textDocument/documentLink' $document)
    (lsp diagnostics 22 $uri)
    (lsp request 23 'workspace/symbol' {query: 'greet'})
    (lsp request 24 'textDocument/implementation' ($document | merge {position: {line: 1, character: 5}}))
]
assert ((lsp result $responses 10).contents.value | str contains 'string')
assert ((lsp result $responses 11).signatures.0.documentation | to nuon | str contains 'Greet a friend')
assert ((lsp result $responses 11).signatures.0.label | str contains 'name: string')
assert equal (lsp result $responses 12).0.range {start: {line: 6, character: 16}, end: {line: 6, character: 21}}
assert equal (lsp result $responses 13).0.range.start {line: 0, character: 13}
let symbols = lsp result $responses 14
assert equal ($symbols | where name == 'greet' | get 0.range.start.line) 6
assert equal ($symbols | where name == 'Box' | get 0.children.0.name) 'size'
assert (((lsp result $responses 15) | length) > 0)
assert equal (lsp result $responses 16).0.range {start: {line: 9, character: 15}, end: {line: 9, character: 20}}
assert equal ((lsp result $responses 17) | length) 2
# TypeScript does not enable inlay hints without preferences.
assert equal (lsp result $responses 18) null
assert (((lsp result $responses 19).data | length) > 0)
assert ('size' in (lsp result $responses 20 | get label))
assert equal (lsp result $responses 21) null
assert equal (lsp result $responses 22).items []
assert equal (lsp result $responses 23).0.location.uri $uri
assert equal (lsp result $responses 24).0.range.start {line: 1, character: 4}
