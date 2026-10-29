use ../../util/test.nu *

# A file checked in destructively leaves a checkout that another user can check in, as an ordinary checkin does, including when the checkout is reused.

# Compare identical files in separate stores so neither checkin reuses the other checkout.
let local_ordinary = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	vfs: false
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let ordinary_id = tg --token $alice.token checkin --no-tokens --no-ignore (artifact 'hello') | referent node
let checkout = tg --token $alice.token checkout $ordinary_id | str trim
assert equal (tg --token $bob.token checkin --no-tokens $checkout | referent node) $ordinary_id
server stop $local_ordinary

# Build output collection uses destructive checkin, which must also authorize the checkout.
let local_destructive = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	vfs: false
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let destructive_id = tg --token $alice.token checkin --no-tokens --destructive --no-ignore (artifact 'hello') | referent node
assert equal $destructive_id $ordinary_id
let checkout = tg --token $alice.token checkout $destructive_id | str trim
assert equal (tg --token $bob.token checkin --no-tokens $checkout | referent node) $destructive_id 'destructive checkin must authorize the checkout'

# Reusing the cached file must retain its authorization.
assert equal (tg --token $alice.token checkout $destructive_id | str trim) $checkout
assert equal (tg --token $bob.token checkin --no-tokens $checkout | referent node) $destructive_id
server stop $local_destructive
