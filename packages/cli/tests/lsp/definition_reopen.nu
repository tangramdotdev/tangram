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
    let target = lsp uri ($path | path join $helper)
    mut session = lsp start
    $session = lsp send_all $session [
        (lsp initialize 1)
        (lsp initialized)
        (lsp notification 'textDocument/didOpen' {textDocument: {uri: $uri, languageId: $case.language, version: 1, text: $case.source}})
        (lsp notification 'textDocument/didOpen' {textDocument: {uri: $target, languageId: $case.language, version: 1, text: $case.text}})
        (lsp definition 10 $uri 1 $case.character)
    ]
    let response = lsp wait_result $session 10
    $session = $response.session
    assert equal $response.result.0.range.start {line: 0, character: $case.definition_character}

    # Reopen with the same editor version, without a checker query between close and open.
    $session = lsp send_all $session [
        (lsp notification 'textDocument/didClose' {textDocument: {uri: $target}})
        (lsp notification 'textDocument/didOpen' {textDocument: {uri: $target, languageId: $case.language, version: 1, text: ("\n\n" + $case.text)}})
        (lsp definition 11 $uri 1 $case.character)
    ]
    let response = lsp wait_result $session 11
    $session = $response.session
    assert equal $response.result.0.range.start {line: 2, character: $case.definition_character}

    # The next editor version must also invalidate the reopened source.
    $session = lsp send_all $session [
        (lsp notification 'textDocument/didChange' {textDocument: {uri: $target, version: 2}, contentChanges: [{text: ("\n\n\n" + $case.text)}]})
        (lsp definition 12 $uri 1 $case.character)
    ]
    let response = lsp wait_result $session 12
    $session = $response.session
    assert equal $response.result.0.range.start {line: 3, character: $case.definition_character}

    $session = lsp send_all $session [
        (lsp notification 'textDocument/didClose' {textDocument: {uri: $target}})
        (lsp definition 13 $uri 1 $case.character)
    ]
    let response = lsp wait_result $session 13
    $session = $response.session
    assert equal $response.result.0.range.start {line: 0, character: $case.definition_character}
    lsp stop $session
}
