use ../../test.nu *

# Destroying a sandbox requires access to it.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

let sandbox = tg --token $alice.token sandbox create --no-network | str trim

let eve_destroy = tg --token $eve.token sandbox destroy $sandbox | complete
failure $eve_destroy "Eve must not destroy a sandbox she cannot access"

let alice_get = tg --token $alice.token sandbox get $sandbox | complete
success $alice_get "Alice's sandbox should still exist after Eve's failed destroy"
