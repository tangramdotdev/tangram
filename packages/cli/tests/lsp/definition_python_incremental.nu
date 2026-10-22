use ../lib/test.nu *
use ../lib/lsp.nu

# Python go-to-definition stays correct across identical queries and changes to unrelated documents.

let local = server spawn
let path = artifact {'main.tg.py': "def greet(): return 42\nvalue = greet()\n"}
let uri = lsp uri ($path | path join main.tg.py)
mut session = lsp start
$session = lsp send_all $session [(lsp initialize 1) (lsp initialized)]
let response = lsp wait_result $session 1
$session = $response.session

$session = lsp send $session (lsp definition 10 $uri 1 10)
let response = lsp wait_result $session 10
$session = $response.session
assert equal $response.result.0.range.start {line: 0, character: 4}

# An identical query returns the same definition.
$session = lsp send $session (lsp definition 11 $uri 1 10)
let response = lsp wait_result $session 11
$session = $response.session
assert equal $response.result.0.range.start {line: 0, character: 4}

# A query for an unrelated document does not change the original module's definition.
let other = artifact {'main.tg.py': "def other(): return 43\nvalue = other()\n"}
let other_uri = lsp uri ($other | path join main.tg.py)
$session = lsp send $session (lsp definition 12 $other_uri 1 10)
let response = lsp wait_result $session 12
$session = $response.session
$session = lsp send $session (lsp definition 13 $uri 1 10)
let response = lsp wait_result $session 13
$session = $response.session
assert equal $response.result.0.range.start {line: 0, character: 4}

# Editing an unrelated file does not change the original module's definition.
"def other(): return 99\nvalue = other()\n" | save --force ($other | path join main.tg.py)
$session = lsp send $session (lsp definition 14 $uri 1 10)
let response = lsp wait_result $session 14
$session = $response.session
assert equal $response.result.0.range.start {line: 0, character: 4}
lsp stop $session
