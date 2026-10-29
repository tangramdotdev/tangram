use ../../util/test.nu *
use ../../util/lsp.nu

# Python go-to-definition into a tagged dependency stays correct after authorization refreshes and follows dependency updates.

let local = server spawn --now '2026-10-05T12:00:00Z'
let dependency = artifact {'tangram.py': 'def greet(): return 42'}
tg tag -p lsp-token/1.0.0 $dependency
rm --recursive $dependency
let path = artifact {'main.tg.py': '# /// script
# [tool.tangram.imports.dep]
# specifier = "lsp-token/^1"
# ///
from dep import greet
value = greet()
'}
let uri = lsp uri ($path | path join main.tg.py)
mut session = lsp start
$session = lsp send_all $session [(lsp initialize 1) (lsp initialized)]
let response = lsp wait_result $session 1
$session = $response.session

$session = lsp send $session (lsp definition 10 $uri 5 10)
let response = lsp wait_result $session 10
$session = $response.session
assert equal $response.result.0.range.start {line: 0, character: 4}

# Refreshing authorization preserves the definition.
advance_time $local 5sec
$session = lsp send $session (lsp definition 11 $uri 5 10)
let response = lsp wait_result $session 11
$session = $response.session
assert equal $response.result.0.range.start {line: 0, character: 4}

# A real dependency update must still invalidate the cached analysis.
let updated = artifact {'tangram.py': "# The updated package.\n\ndef greet(): return 43"}
tg tag -p lsp-token/1.1.0 $updated
rm --recursive $updated
tg index
success (tg update ($path | path join main.tg.py) | complete)
$session = lsp send $session (lsp definition 12 $uri 5 10)
let response = lsp wait_result $session 12
$session = $response.session
assert equal ($response.result | length) 1
assert equal $response.result.0.range.start {line: 2, character: 4}
lsp stop $session
