use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let source = "import { greet } from './helper.tg.py';\ngreet();\n"
let path = artifact {
    'main.tg.ts': $source
    'helper.tg.py': "def greet():\n    return \"a sufficiently long string\"\n"
}
let uri = lsp uri ($path | path join main.tg.ts)
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp did_open $uri $source)
    (lsp definition 10 $uri 1 2)
    (lsp request 11 textDocument/prepareRename {textDocument: {uri: $uri}, position: {line: 1, character: 2}})
    (lsp rename 12 $uri 1 2 welcome)
]
let location = (lsp result $responses 10).0
let generated = lsp path_for_uri $location.uri
assert ($generated | str contains '/generated/')
assert ($generated | str ends-with '.tg.js')
let text = open --raw $generated
let line = $text | lines | get $location.range.start.line
assert equal ($line | str substring $location.range.start.character..<$location.range.end.character) 'f0'
assert ($line | str contains 'const f0 =')
assert equal (lsp result $responses 11) null
assert equal (lsp result $responses 12) null
assert equal (open --raw ($path | path join helper.tg.py)) (doc "def greet():\n    return \"a sufficiently long string\"\n")

# Opening the generated module must not allow edits through rename.
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp did_open $location.uri $text)
    (lsp rename 10 $location.uri $location.range.start.line $location.range.start.character welcome)
]
assert equal (lsp result $responses 10) null
