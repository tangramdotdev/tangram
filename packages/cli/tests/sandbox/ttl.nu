use ../../test.nu *

# A sandbox defaults to a five-minute ttl, supports an explicitly infinite ttl, and is destroyed after its ttl expires.

let server = spawn --config { cleaner: {}, sandbox: { ttl: 0 } }

let default = tg sandbox create | str trim
let sandbox = tg sandbox get $default | from json
assert equal $sandbox.ttl 300 "the default ttl should be five minutes"
tg sandbox destroy $default

let infinite = tg sandbox create --no-ttl | str trim
let sandbox = tg sandbox get $infinite | from json
assert ($sandbox.ttl? | is-empty) "an explicitly infinite ttl should be preserved"
tg sandbox destroy $infinite

let id = tg sandbox create --ttl 1s | str trim

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
