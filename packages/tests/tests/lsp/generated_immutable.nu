use ../../util/test.nu *
use ../../util/lsp.nu

# Cross-language bindings reject edits even when opened with unsaved text.

let local = server spawn
let javascript = "import { greet } from './helper.tg.py';\ngreet();"
let python = '# /// script
# [tool.tangram.dependencies.friend]
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

# Find the bindings directory through the JavaScript bindings of the Python helper.
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp did_open (lsp uri ($path | path join main.tg.ts)) $javascript)
    (lsp request 10 workspace/symbol {query: greet})
]
let bindings_directory = lsp result $responses 10 | where { |symbol| $symbol.location.uri | str contains '/bindings/' } | get 0.location.uri | lsp path_for_uri $in | path dirname

for case in [
    {main: main.tg.ts, control: control.tg.ts, source: "import { greet } from './helper.tg.ts';\nexport function retained() { return 1; }", only: 'source.organizeImports'}
    {main: main.tg.py, control: control.tg.py, source: "missing_name()\n", only: quickfix}
] {
    let uri = lsp uri ($path | path join $case.main)
    let control_uri = lsp uri ($path | path join $case.control)
    mut session = lsp start
    $session = lsp send_all $session [
        (lsp initialize 1)
        (lsp initialized)
        (lsp did_open $uri (open --raw ($path | path join $case.main)))
    ]

    # Open a module in the bindings directory with unsaved text.
    let bindings_path = if ($case.main | str ends-with '.py') { $bindings_directory | path join 'immutable.tg.py' } else { $bindings_directory | path join 'immutable.tg.js' }
    $case.source | save --force $bindings_path
    let bindings_uri = lsp uri $bindings_path
    let original = open --raw $bindings_path
    $session = lsp send_all $session [
        (lsp did_open $control_uri $case.source)
        (lsp did_open $bindings_uri $case.source)
        (lsp request 11 'textDocument/codeAction' {textDocument: {uri: $control_uri}, range: {start: {line: 0, character: 0}, end: {line: 1, character: 0}}, context: {diagnostics: [], only: [$case.only]}})
        (lsp request 12 'textDocument/codeAction' {textDocument: {uri: $bindings_uri}, range: {start: {line: 0, character: 0}, end: {line: 1, character: 0}}, context: {diagnostics: [], only: [$case.only]}})
        (lsp request 13 'textDocument/prepareRename' {textDocument: {uri: $bindings_uri}, position: {line: 0, character: 2}})
        (lsp rename 14 $bindings_uri 0 2 renamed)
        (lsp request 15 'textDocument/formatting' {textDocument: {uri: $bindings_uri}, options: {tabSize: 4, insertSpaces: true}})
        (lsp request 16 'textDocument/completion' {textDocument: {uri: $bindings_uri}, position: {line: 0, character: 2}})
    ]
    let response = lsp wait_result $session 11
    $session = $response.session
    assert (($response.result | length) > 0)
    for id in 12..16 {
        let response = lsp wait_result $session $id
        $session = $response.session
        assert equal $response.result null
    }
    assert equal (open --raw $bindings_path) $original
    lsp stop $session
}
