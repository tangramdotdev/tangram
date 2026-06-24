use ../../test.nu *

# Two sandbox owners must not see or control each other's sandboxes.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

let alice_sandbox = tg --token $alice.token sandbox create --no-network | str trim
let eve_sandbox = tg --token $eve.token sandbox create --no-network | str trim

success (tg --token $alice.token sandbox get $alice_sandbox | complete) "Alice should get her own sandbox"
failure (tg --token $alice.token sandbox get $eve_sandbox | complete) "Alice must not get Eve's sandbox"
success (tg --token $eve.token sandbox get $eve_sandbox | complete) "Eve should get her own sandbox"
failure (tg --token $eve.token sandbox get $alice_sandbox | complete) "Eve must not get Alice's sandbox"

let alice_list = tg --token $alice.token sandbox list | from json
assert (($alice_list | where id == $alice_sandbox | is-empty) == false) "Alice should see her own sandbox"
assert ($alice_list | where id == $eve_sandbox | is-empty) "Alice must not see Eve's sandbox"

let eve_list = tg --token $eve.token sandbox list | from json
assert (($eve_list | where id == $eve_sandbox | is-empty) == false) "Eve should see her own sandbox"
assert ($eve_list | where id == $alice_sandbox | is-empty) "Eve must not see Alice's sandbox"

failure (tg --token $alice.token sandbox destroy $eve_sandbox | complete) "Alice must not destroy Eve's sandbox"
failure (tg --token $eve.token sandbox destroy $alice_sandbox | complete) "Eve must not destroy Alice's sandbox"

success (tg --token $alice.token sandbox get $alice_sandbox | complete) "Alice's sandbox should still exist"
success (tg --token $eve.token sandbox get $eve_sandbox | complete) "Eve's sandbox should still exist"
