use ../../util/test.nu *
use ../../util/lsp.nu

let source = "const value = 1; const object = {value}; object.value;\nconst record = {\"123\": 1}; record[\"123\"];\n"
let local = server spawn
let path = artifact {'main.tg.ts': $source}
let uri = lsp uri ($path | path join main.tg.ts)
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp did_open $uri $source)
    (lsp rename 10 $uri 0 48 fresh)
    (lsp rename 11 $uri 1 17 '456')
]
let edits = (lsp result $responses 10).documentChanges.0.edits
assert equal ($edits | length) 2
let shorthand = $edits | where range.start.character == 33 | get 0
assert ($shorthand.newText | str contains fresh)
assert ($shorthand.newText | str contains value)
for id in [10, 11] {
    let edits = (lsp result $responses $id).documentChanges.0.edits
    mut lines = $source | split row "\n"
    for edit in ($edits | sort-by {get range.start.line} {get range.start.character} --reverse) {
        assert equal $edit.range.start.line $edit.range.end.line
        let line = $lines | get $edit.range.start.line
        let prefix = $line | str substring 0..<$edit.range.start.character
        let suffix = $line | str substring $edit.range.end.character..
        $lines = $lines | update $edit.range.start.line ($prefix + $edit.newText + $suffix)
    }
    $lines | str join "\n" | save --force ($path | path join main.tg.ts)
    success (tg check ($path | path join main.tg.ts) | complete)
}
