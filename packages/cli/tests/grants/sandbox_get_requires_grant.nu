use ../../test.nu *

# Getting a sandbox requires access to it.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

let sandbox = tg --token $alice.token sandbox create --no-network | str trim

let alice_get = tg --token $alice.token sandbox get $sandbox | complete
success $alice_get "Alice should get her own sandbox"

let eve_get = tg --token $eve.token sandbox get $sandbox | complete
failure $eve_get "Eve must not get a sandbox she cannot access"
snapshot $eve_get.stdout ''
