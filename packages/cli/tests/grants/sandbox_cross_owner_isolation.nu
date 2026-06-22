use ../../test.nu *

# Sandbox ownership is isolated between principals in both directions: when two principals each create a sandbox, neither can get, list, or destroy the other's. Each principal sees and controls only its own sandbox, even while both coexist.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Each principal creates a sandbox.
let alice_sandbox = tg --token $alice.token sandbox create --no-network | str trim
let eve_sandbox = tg --token $eve.token sandbox create --no-network | str trim

# Each principal can get only their own sandbox.
success (tg --token $alice.token sandbox get $alice_sandbox | complete) "Alice should get her own sandbox."
failure (tg --token $alice.token sandbox get $eve_sandbox | complete) "Alice must not get Eve's sandbox."
success (tg --token $eve.token sandbox get $eve_sandbox | complete) "Eve should get her own sandbox."
failure (tg --token $eve.token sandbox get $alice_sandbox | complete) "Eve must not get Alice's sandbox."

# Each principal's list contains only their own sandbox.
let alice_list = tg --token $alice.token sandbox list | from json
assert (($alice_list | where id == $alice_sandbox | is-empty) == false) "Alice should see her own sandbox."
assert ($alice_list | where id == $eve_sandbox | is-empty) "Alice must not see Eve's sandbox."
let eve_list = tg --token $eve.token sandbox list | from json
assert (($eve_list | where id == $eve_sandbox | is-empty) == false) "Eve should see her own sandbox."
assert ($eve_list | where id == $alice_sandbox | is-empty) "Eve must not see Alice's sandbox."

# Neither principal can destroy the other's sandbox.
failure (tg --token $alice.token sandbox destroy $eve_sandbox | complete) "Alice must not destroy Eve's sandbox."
failure (tg --token $eve.token sandbox destroy $alice_sandbox | complete) "Eve must not destroy Alice's sandbox."

# Each sandbox still exists after the cross-owner destroy attempts.
success (tg --token $alice.token sandbox get $alice_sandbox | complete) "Alice's sandbox should still exist."
success (tg --token $eve.token sandbox get $eve_sandbox | complete) "Eve's sandbox should still exist."
