use ../../test.nu *

# Listing sandboxes is scoped to the principal's visible sandboxes.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

let sandbox = tg --token $alice.token sandbox create --no-network | str trim

let alice_list = tg --token $alice.token sandbox list | from json
assert (($alice_list | where id == $sandbox | is-empty) == false) "Alice should see her own sandbox"

let eve_list = tg --token $eve.token sandbox list | from json
assert ($eve_list | where id == $sandbox | is-empty) "Eve must not see a sandbox she cannot access"

tg --token $alice.token sandbox destroy $sandbox
