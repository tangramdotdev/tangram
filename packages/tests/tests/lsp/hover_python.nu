use ../../util/test.nu *
use ../../util/lsp.nu

# Hovering in a Python module shows the type of the name under the cursor.

let local = server spawn
let source = "def greet(name: str) -> str:\n    return name\n\ngreet(\"World\")\n"
let path = artifact {'main.tg.py': $source}
let uri = lsp uri ($path | path join main.tg.py)
mut session = lsp start
$session = lsp send_all $session [
    (lsp initialize 1)
    (lsp initialized)
    (lsp notification 'textDocument/didOpen' {textDocument: {uri: $uri, languageId: 'python', version: 1, text: $source}})
    (lsp hover 10 $uri 3 1)
]
let response = lsp wait_result $session 10
$session = $response.session
let value = $response.result.contents.value
assert ($value | str contains 'def greet(name: str) -> str') $value
lsp stop $session
