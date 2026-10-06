use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let path = artifact {'main.tg.py': "directory = tg.Directory()\ncount = len([])\n"}
let uri = lsp uri ($path | path join main.tg.py)
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp definition 10 $uri 0 17)
    (lsp definition 11 $uri 1 9)
]
for pair in [[10 'class Directory'] [11 'def len(']] {
    let locations = lsp result $responses $pair.0
    assert (($locations | length) > 0)
    let location = $locations.0
    let file = lsp path_for_uri $location.uri
    assert ($file | path exists)
    let text = open --raw $file
    assert ($text | lines | get $location.range.start.line | str contains $pair.1)
    # Materialized Python library files can be opened and queried again.
    let responses = lsp exchange [
        (lsp initialize 1)
        (lsp initialized)
        (lsp notification 'textDocument/didOpen' {textDocument: {uri: $location.uri, languageId: 'python', version: 1, text: $text}})
        (lsp definition 20 $location.uri $location.range.start.line $location.range.start.character)
    ]
    lsp response $responses 20 | ignore
}
