use ../lib/test.nu *
use ../lib/lsp.nu

# Wait until the server log contains every record that the server wrote before this call, by requesting a uniquely traced match and following the log until it appears.
def drain_log [server: record] {
	let marker = $'logmarker(random chars | str lowercase)'
	tg match $marker | complete | ignore
	wait_for_log $server $marker
}

$env.TANGRAM_TRACING = 'tangram_compiler::python=debug,ruff_db::parsed=trace,ty_python_semantic::types::infer=trace,tangram_server::match_=trace'
let local = server spawn
let path = artifact {'main.tg.py': "def greet(): return 42\nvalue = greet()\n"}
let uri = lsp uri ($path | path join main.tg.py)
mut session = lsp start
$session = lsp send_all $session [(lsp initialize 1) (lsp initialized)]
let response = lsp wait_result $session 1
$session = $response.session
assert not (open --raw $local.log | str contains 'created the Python database')

$session = lsp send $session (lsp definition 10 $uri 1 10)
let response = lsp wait_result $session 10
$session = $response.session
assert equal $response.result.0.range.start {line: 0, character: 4}
drain_log $local
let before = open --raw $local.log
let parsed = $before | lines | where {|line| $line | str contains 'parsed_module'} | length
let inferred = $before | lines | where {|line| $line | str contains 'infer_'} | length
assert ($parsed > 0)
assert ($inferred > 0)

# Identical queries reuse both parsing and inference, not just the worker thread.
$session = lsp send $session (lsp definition 11 $uri 1 10)
let response = lsp wait_result $session 11
$session = $response.session
assert equal $response.result.0.range.start {line: 0, character: 4}
drain_log $local
let after = open --raw $local.log
assert equal ($after | lines | where {|line| $line | str contains 'parsed_module'} | length) $parsed
assert equal ($after | lines | where {|line| $line | str contains 'infer_'} | length) $inferred
assert equal ($after | lines | where {|line| $line | str contains 'created the Python database'} | length) 1

# An unrelated document does not cause the original module to be reparsed or reinferred.
let other = artifact {'main.tg.py': "def other(): return 43\nvalue = other()\n"}
let other_uri = lsp uri ($other | path join main.tg.py)
$session = lsp send $session (lsp definition 12 $other_uri 1 10)
let response = lsp wait_result $session 12
$session = $response.session
drain_log $local
let before = open --raw $local.log
$session = lsp send $session (lsp definition 13 $uri 1 10)
let response = lsp wait_result $session 13
$session = $response.session
drain_log $local
let after = open --raw $local.log
for query in ['parsed_module' 'infer_'] {
    assert equal ($after | lines | where {|line| $line | str contains $query} | length) ($before | lines | where {|line| $line | str contains $query} | length)
}
# Editing an unrelated file also preserves the original module's analysis.
"def other(): return 99\nvalue = other()\n" | save --force ($other | path join main.tg.py)
let before = open --raw $local.log
$session = lsp send $session (lsp definition 14 $uri 1 10)
let response = lsp wait_result $session 14
$session = $response.session
drain_log $local
let after = open --raw $local.log
for query in ['parsed_module' 'infer_'] {
    assert equal ($after | lines | where {|line| $line | str contains $query} | length) ($before | lines | where {|line| $line | str contains $query} | length)
}
lsp stop $session
