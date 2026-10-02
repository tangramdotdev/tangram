use ../lib/test.nu *

# A sandbox defaults to a five-minute ttl, supports an explicitly infinite ttl, and is destroyed after its ttl expires.

let local = server spawn --name local --config { indexer: { cleaning: {} }, runner: { sandbox_state_ttl: 0 }, sandbox: { ttl: 0 } }

let default = tg sandbox create --no-tokens | referent node
let sandbox = tg sandbox get $default | from json | get data
assert equal $sandbox.ttl 300 "the default ttl should be five minutes"
tg sandbox destroy $default

let infinite = tg sandbox create --no-tokens --no-ttl | referent node
let sandbox = tg sandbox get $infinite | from json | get data
assert ($sandbox.ttl? | is-empty) "an explicitly infinite ttl should be preserved"
tg sandbox destroy $infinite

let id = tg sandbox create --no-tokens --ttl 1s | referent node

let output = tg sandbox get $id | complete
success $output

wait_until { (tg sandbox get $id | complete | get exit_code) != 0 } --timeout 15sec "the sandbox should expire"
let output = tg sandbox get $id | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to find the sandbox
	   sandbox = sbx_0000000000000000000000000000

'

# Expired index entries and expired runner state leave no sandbox endpoints available.
let socket = $local.url | str replace 'http+unix://' '' | url decode
let processes = http get --allow-errors --full --max-time 10sec --unix-socket $socket $'http://localhost/sandboxes/($id)/processes?timeout=0'
assert equal $processes.status 404
let status = http get --allow-errors --full --max-time 10sec --unix-socket $socket $'http://localhost/sandboxes/($id)/status?timeout=0'
assert equal $status.status 404
let output = timeout 10s tg sandbox wait $id | complete
failure $output
assert ($output.stderr | str contains "failed to find the sandbox")
