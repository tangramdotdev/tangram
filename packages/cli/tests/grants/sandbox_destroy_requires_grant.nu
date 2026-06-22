use ../../test.nu *

# Destroying a sandbox must require access to it: a principal who did not create a sandbox, and was not granted access to it, must not be able to destroy it by id. Eve's destroy is masked as not found, and Alice's sandbox still exists afterward.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice creates a sandbox.
let sandbox = tg --token $alice.token sandbox create --no-network | str trim

# Eve must not be able to destroy Alice's sandbox.
let eve_destroy = tg --token $eve.token sandbox destroy $sandbox | complete
failure $eve_destroy "Eve must not destroy a sandbox she did not create."

# Alice's sandbox still exists.
let alice_get = tg --token $alice.token sandbox get $sandbox | complete
success $alice_get "Alice's sandbox should still exist after Eve's failed destroy."
