use ../../util/test.nu *
use ../../util/lsp.nu

# Navigating to a config module or to a symbol imported from it goes to the config's interface file, and references do not include its bindings.

let local = server spawn
let source = "import { greet } from './greet.tg.json';\ngreet.build('World');\n"
let path = artifact {
    'main.tg.ts': $source
    'greet.tg.json': '{ "runtime": "./runtime" }'
    'greet.tgi.ts': "export declare const greet: tg.Command<[string], string>;\n"
    'runtime': { 'tangram.ts': 'export default async () => "Hello!";' }
}
let uri = lsp uri ($path | path join main.tg.ts)
let interface_uri = lsp uri ($path | path join greet.tgi.ts)
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp did_open $uri $source)
    (lsp definition 10 $uri 0 27)
    (lsp definition 11 $uri 1 2)
    (lsp definition 13 $uri 0 39)
    (lsp request 14 workspace/symbol {query: greet})
    (lsp request 15 textDocument/documentLink {textDocument: {uri: $uri}})
    (lsp request 12 textDocument/references {textDocument: {uri: $uri}, position: {line: 1, character: 2}, context: {includeDeclaration: true}})
]

# The definition of the specifier is the start of the interface file.
assert equal (lsp result $responses 10) [{
    uri: $interface_uri
    range: {start: {line: 0, character: 0}, end: {line: 0, character: 0}}
}]

# The definition of the imported symbol is its name in the interface file.
assert equal (lsp result $responses 11) [{
    uri: $interface_uri
    range: {start: {line: 0, character: 21}, end: {line: 0, character: 26}}
}]

# The references include the name in the interface file and none in bindings.
let references = lsp result $responses 12
assert ($references | any { |reference| $reference.uri == $interface_uri }) ($references | to json)
assert not ($references | any { |reference| $reference.uri | str contains '/bindings/' }) ($references | to json)

# The definition of the end of the import statement is the start of the interface file.
assert equal (lsp result $responses 13) [{
    uri: $interface_uri
    range: {start: {line: 0, character: 0}, end: {line: 0, character: 0}}
}]

# Workspace search finds the export in the interface file and never finds bindings.
let symbols = lsp result $responses 14
assert ($symbols | any { |symbol| $symbol.location.uri == $interface_uri }) ($symbols | to json)
assert not ($symbols | any { |symbol| $symbol.location.uri | str contains '/bindings/' }) ($symbols | to json)

# The link of the import's specifier targets the interface file.
let links = lsp result $responses 15
assert equal ($links | get 0.target) $interface_uri
