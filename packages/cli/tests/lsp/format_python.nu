use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let path = artifact {'main.tg.py': 'value = 0'}
let module = $path | path join main.tg.py
let uri = lsp uri $module
let source = 'value=  "🙂"'

# Formatting uses the open document, with a range measured in UTF-16 code units.
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp notification 'textDocument/didOpen' {
        textDocument: {uri: $uri, languageId: 'python', version: 1, text: $source}
    })
    (lsp request 10 'textDocument/formatting' {
        textDocument: {uri: $uri}
        options: {tabSize: 4, insertSpaces: true}
    })
]
let edits = lsp result $responses 10
assert equal ($edits | length) 1
assert equal $edits.0.newText ('value = "🙂"' + (char nl))
assert equal $edits.0.range {
    start: {line: 0, character: 0}
    end: {line: 0, character: 12}
}
assert equal (open --raw $module) 'value = 0'
