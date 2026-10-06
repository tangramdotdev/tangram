use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
for layout in [direct reexport cycle] {
    for case in [
        {extension: ts, language: typescript, text: "export function greet() { return 42; }\nconst value = greet();\n", line: 1, character: 16, definition_character: 16}
        {extension: py, language: python, text: "def greet(): return 42\nvalue = greet()\n", line: 5, character: 10, definition_character: 4}
    ] {
        let filename = $'tangram.($case.extension)'
        let dependency = if $layout == direct {
            artifact { $filename: $case.text }
        } else if $case.extension == ts {
            let suffix = if $layout == cycle { "import './tangram.ts';" } else { '' }
            artifact {
                'tangram.ts': "export { greet } from './helper.tg.ts';"
                'helper.tg.ts': ($case.text + $suffix)
            }
        } else {
            let suffix = if $layout == cycle { 'from . import greet as other' } else { '' }
            artifact {
                'tangram.py': 'from .helper import greet'
                'helper.tg.py': ($case.text + $suffix)
            }
        }
        let id = tg checkin ($dependency | path join $filename)
        let source = if $case.extension == ts {
            $'import { greet } from "($id)";(char nl)const value = greet();'
        } else {
            $'# /// script
# [tool.tangram.imports.dep]
# specifier = "($id)"
# ///
from dep import greet
value = greet()
'
        }
        let filename = $'main.tg.($case.extension)'
        let path = artifact { $filename: $source }
        let uri = lsp uri ($path | path join $filename)
        mut session = lsp start
        $session = lsp send_all $session [
            (lsp initialize 1)
            (lsp initialized)
            (lsp notification 'textDocument/didOpen' {textDocument: {uri: $uri, languageId: $case.language, version: 0, text: $source}})
            (lsp definition 10 $uri $case.line $case.character)
        ]
        let response = lsp wait_result $session 10
        $session = $response.session
        assert equal ($response.result | length) 1
        let target = $response.result.0.uri
        assert equal $response.result.0.range.start {line: 0, character: $case.definition_character}

        # The materialized URI must share the resolved dependency's buffer despite different referent options or graph representations.
        $session = lsp send_all $session [
            (lsp notification 'textDocument/didOpen' {textDocument: {uri: $target, languageId: $case.language, version: 0, text: ("\n\n" + $case.text)}})
            (lsp definition 11 $target 3 $case.character)
        ]
        let response = lsp wait_result $session 11
        $session = $response.session
        assert equal $response.result.0.range.start {line: 2, character: $case.definition_character}

        # Both the importer and the materialized module must see subsequent buffer edits.
        $session = lsp send_all $session [
            (lsp notification 'textDocument/didChange' {textDocument: {uri: $target, version: 1}, contentChanges: [{text: ("\n\n\n" + $case.text)}]})
            (lsp definition 12 $uri $case.line $case.character)
        ]
        let response = lsp wait_result $session 12
        $session = $response.session
        assert equal $response.result.0.range.start {line: 3, character: $case.definition_character}

        # Closing the buffer restores the immutable artifact contents.
        $session = lsp send_all $session [
            (lsp notification 'textDocument/didClose' {textDocument: {uri: $target}})
            (lsp definition 13 $uri $case.line $case.character)
        ]
        let response = lsp wait_result $session 13
        $session = $response.session
        assert equal $response.result.0.range.start {line: 0, character: $case.definition_character}
        lsp stop $session
    }
}
