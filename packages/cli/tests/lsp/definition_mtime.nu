use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
for case in [
    {extension: ts, language: typescript, text: 'export function greet() { return 42; }', source: "import { greet } from './helper.tg.ts';\nconst value = greet();", character: 16, definition_character: 16}
    {extension: py, language: python, text: 'def greet(): return 42', source: "from .helper import greet\nvalue = greet()", character: 10, definition_character: 4}
] {
    let root = $'tangram.($case.extension)'
    let helper = $'helper.tg.($case.extension)'
    let path = artifact {$root: $case.source, $helper: $case.text}
    let uri = lsp uri ($path | path join $root)
    let helper = $path | path join $helper
    python3 -c 'import os, sys; os.utime(sys.argv[1], (946684800, 946684800))' $helper
    mut session = lsp start
    $session = lsp send_all $session [
        (lsp initialize 1)
        (lsp initialized)
        (lsp notification 'textDocument/didOpen' {textDocument: {uri: $uri, languageId: $case.language, version: 1, text: $case.source}})
        (lsp definition 10 $uri 1 $case.character)
    ]
    let response = lsp wait_result $session 10
    $session = $response.session
    assert equal $response.result.0.range.start {line: 0, character: $case.definition_character}

    # A dependency restored with an older timestamp must invalidate its cached analysis.
    ("\n\n" + $case.text) | save --force $helper
    python3 -c 'import os, sys; os.utime(sys.argv[1], (946684799, 946684799))' $helper
    $session = lsp send $session (lsp definition 11 $uri 1 $case.character)
    let response = lsp wait_result $session 11
    $session = $response.session
    assert equal $response.result.0.range.start {line: 2, character: $case.definition_character}
    lsp stop $session
}
