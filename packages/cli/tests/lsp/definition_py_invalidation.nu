use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let path = artifact {'tangram.py': "from .helper import greet\nvalue = greet()\n"}
let uri = lsp uri ($path | path join tangram.py)
let helper = $path | path join helper.tg.py
let helper_uri = lsp uri $helper
mut session = lsp start
$session = lsp send_all $session [(lsp initialize 1) (lsp initialized)]
let response = lsp wait_result $session 1
$session = $response.session

# A missing import must be re-resolved after a file appears, without editing the importer.
$session = lsp send $session (lsp definition 10 $uri 1 10)
let response = lsp wait_result $session 10
$session = $response.session
"def greet(): return 42\n" | save $helper
$session = lsp send $session (lsp definition 11 $uri 1 10)
let response = lsp wait_result $session 11
$session = $response.session
assert equal $response.result [{uri: $helper_uri, range: {start: {line: 0, character: 4}, end: {line: 0, character: 9}}}]

# Deletion and recreation invalidate cached file status and resolution.
rm $helper
$session = lsp send $session (lsp definition 12 $uri 1 10)
let response = lsp wait_result $session 12
$session = $response.session
assert ($response.result | default [] | all {|location| $location.uri != $helper_uri})
"\n\ndef greet(): return 43\n" | save $helper
$session = lsp send $session (lsp definition 13 $uri 1 10)
let response = lsp wait_result $session 13
$session = $response.session
assert equal $response.result.0.range.start {line: 2, character: 4}

# Changing only the importer's metadata changes the resolved dependency.
let other = artifact {'helper.tg.py': "# Another dependency.\ndef greet(): return 44\n"}
let other_uri = lsp uri ($other | path join helper.tg.py)
let source = $'# /// script
# [tool.tangram.imports.dep]
# specifier = "($other)/helper.tg.py"
# ///
from dep import greet
value = greet()
'
$session = lsp send_all $session [
    (lsp notification 'textDocument/didOpen' {textDocument: {uri: $uri, languageId: 'python', version: 1, text: $source}})
    (lsp definition 14 $uri 5 10)
]
let response = lsp wait_result $session 14
$session = $response.session
assert equal $response.result.0.uri $other_uri
assert equal $response.result.0.range.start {line: 1, character: 4}
lsp stop $session
