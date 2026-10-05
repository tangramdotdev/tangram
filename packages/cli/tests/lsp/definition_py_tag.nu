use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let first = artifact {'tangram.py': 'def greet(): return 42'}
tg tag -p incremental/1.0.0 $first
rm --recursive $first
let path = artifact {'tangram.py': '# /// script
# [tool.tangram.imports.dep]
# specifier = "incremental/^1"
# ///
from dep import greet
value = greet()
'}
tg checkin $path | ignore
let uri = lsp uri ($path | path join tangram.py)
mut session = lsp start
$session = lsp send_all $session [(lsp initialize 1) (lsp initialized)]
let response = lsp wait_result $session 1
$session = $response.session
$session = lsp send $session (lsp definition 10 $uri 5 10)
let response = lsp wait_result $session 10
$session = $response.session
assert equal $response.result.0.range.start {line: 0, character: 4}

# Updating the lockfile changes the target without changing the importing source.
let second = artifact {'tangram.py': "# The updated package.\n\ndef greet(): return 43"}
tg tag -p incremental/1.1.0 $second
rm --recursive $second
tg index
success (tg update $path | complete)
$session = lsp send $session (lsp definition 11 $uri 5 10)
let response = lsp wait_result $session 11
$session = $response.session
assert equal ($response.result | length) 1
assert equal $response.result.0.range.start {line: 2, character: 4}
let target = lsp path_for_uri $response.result.0.uri
assert (open --raw $target | str contains 'return 43')
lsp stop $session
