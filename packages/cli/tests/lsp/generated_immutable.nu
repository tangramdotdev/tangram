use ../lib/test.nu *
use ../lib/lsp.nu

# Generated cross-language wrappers reject edits even when opened with unsaved text.

let local = server spawn
let javascript = "import { greet } from './helper.tg.py';\ngreet();"
let python = '# /// script
# [tool.tangram.imports.friend]
# specifier = "./helper.tg.ts"
# ///
from friend import greet
greet()
'
let path = artifact {
    'main.tg.ts': $javascript
    'main.tg.py': $python
    'helper.tg.ts': 'export function greet() { return 42; }'
    'helper.tg.py': "def greet():\n    return 42\n"
    'control.tg.ts': ''
    'control.tg.py': ''
}
for case in [
    {main: main.tg.ts, line: 1, control: control.tg.ts, source: "import { greet } from './helper.tg.ts';\nexport function retained() { return 1; }", only: 'source.organizeImports'}
    {main: main.tg.py, line: 5, control: control.tg.py, source: "missing_name()\n", only: quickfix}
] {
    let uri = lsp uri ($path | path join $case.main)
    let control_uri = lsp uri ($path | path join $case.control)
    mut session = lsp start
    $session = lsp send_all $session [
        (lsp initialize 1)
        (lsp initialized)
        (lsp did_open $uri (open --raw ($path | path join $case.main)))
        (lsp definition 10 $uri $case.line 2)
    ]
    let response = lsp wait_result $session 10
    $session = $response.session
    let generated_uri = $response.result.0.uri
    let generated_path = lsp path_for_uri $generated_uri
    assert ($generated_path | str contains '/generated/')
    let original = open --raw $generated_path
    $session = lsp send_all $session [
        (lsp did_open $control_uri $case.source)
        (lsp did_open $generated_uri $case.source)
        (lsp request 11 'textDocument/codeAction' {textDocument: {uri: $control_uri}, range: {start: {line: 0, character: 0}, end: {line: 1, character: 0}}, context: {diagnostics: [], only: [$case.only]}})
        (lsp request 12 'textDocument/codeAction' {textDocument: {uri: $generated_uri}, range: {start: {line: 0, character: 0}, end: {line: 1, character: 0}}, context: {diagnostics: [], only: [$case.only]}})
        (lsp request 13 'textDocument/prepareRename' {textDocument: {uri: $generated_uri}, position: {line: 0, character: 2}})
        (lsp rename 14 $generated_uri 0 2 renamed)
        (lsp request 15 'textDocument/formatting' {textDocument: {uri: $generated_uri}, options: {tabSize: 4, insertSpaces: true}})
        (lsp request 16 'textDocument/completion' {textDocument: {uri: $generated_uri}, position: {line: 0, character: 2}})
    ]
    let response = lsp wait_result $session 11
    $session = $response.session
    assert (($response.result | length) > 0)
    for id in 12..16 {
        let response = lsp wait_result $session $id
        $session = $response.session
        assert equal $response.result null
    }
    assert equal (open --raw $generated_path) $original
    lsp stop $session
}
