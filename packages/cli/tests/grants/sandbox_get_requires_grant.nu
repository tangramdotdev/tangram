use ../../test.nu *

# Getting a sandbox must require access to it: a principal who did not create a sandbox, and was not granted access to it, must not be able to read it by id. An inaccessible sandbox is masked as not found, so the get returns nothing.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice creates a sandbox.
let sandbox = tg --token $alice.token sandbox create --no-network | str trim

# Sanity: Alice can get her own sandbox.
let alice_get = tg --token $alice.token sandbox get $sandbox | complete
success $alice_get "Alice should get her own sandbox."

# Eve must not be able to get Alice's sandbox; it is masked as not found.
let eve_get = tg --token $eve.token sandbox get $sandbox | complete
failure $eve_get "Eve must not get a sandbox she did not create."
snapshot $eve_get.stdout ''
