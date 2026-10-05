use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let source = "export function greet() { return 42; }\ngreet();\n"
let path = artifact {'main.tg.ts': $source}
let uri = lsp uri ($path | path join main.tg.ts)
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp did_open $uri $source)
    (lsp request 10 'textDocument/documentSymbol' {textDocument: {uri: $uri}})
    (lsp request 11 'textDocument/references' {textDocument: {uri: $uri}, position: {line: 1, character: 2}, context: {includeDeclaration: true}})
    (lsp request 12 'textDocument/references' {textDocument: {uri: $uri}, position: {line: 1, character: 2}, context: {includeDeclaration: false}})
]
assert equal (lsp result $responses 10).0.selectionRange {start: {line: 0, character: 0}, end: {line: 0, character: 38}}
assert equal ((lsp result $responses 11) | length) 2
assert equal (lsp result $responses 12) [{uri: $uri, range: {start: {line: 1, character: 0}, end: {line: 1, character: 5}}}]

# Querying an overloaded function at a use excludes every overload declaration.
let overloaded = "export function pick(value: string): string;\nexport function pick(value: number): number;\nexport function pick(value: string | number) { return value; }\npick(\"x\");\n"
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp did_open $uri $overloaded)
    (lsp request 20 'textDocument/references' {textDocument: {uri: $uri}, position: {line: 3, character: 2}, context: {includeDeclaration: false}})
]
assert equal (lsp result $responses 20) [{uri: $uri, range: {start: {line: 3, character: 0}, end: {line: 3, character: 4}}}]
