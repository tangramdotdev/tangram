use ../lib/test.nu *
use ../lib/lsp.nu

$env.TANGRAM_TRACING = 'tangram_compiler::python=debug,ruff_db::parsed=trace,ty_python_semantic::types::infer=trace'
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
assert not (open --raw $local.log | str contains 'created the Python database')

$session = lsp send $session (lsp definition 10 $uri 5 10)
let response = lsp wait_result $session 10
$session = $response.session
assert equal $response.result.0.range.start {line: 0, character: 4}
# Wait for the server's output collector to drain the tracing records.
sleep 100ms
let before = open --raw $local.log
let parsed = $before | lines | where {|line| $line | str contains 'parsed_module'} | length
let inferred = $before | lines | where {|line| $line | str contains 'infer_'} | length
assert ($parsed > 0)
assert ($inferred > 0)

# Refreshing authorization must preserve both parsing and inference.
advance_time $local 5sec
$session = lsp send $session (lsp definition 11 $uri 5 10)
let response = lsp wait_result $session 11
$session = $response.session
assert equal $response.result.0.range.start {line: 0, character: 4}
sleep 100ms
let after = open --raw $local.log
assert equal ($after | lines | where {|line| $line | str contains 'parsed_module'} | length) $parsed
assert equal ($after | lines | where {|line| $line | str contains 'infer_'} | length) $inferred
assert equal ($after | lines | where {|line| $line | str contains 'created the Python database'} | length) 1

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
