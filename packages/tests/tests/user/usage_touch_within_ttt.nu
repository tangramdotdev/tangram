use ../../util/test.nu *

# Touching another user's object within the object's ttt still charges the toucher.

let local = server spawn --now 2026-01-01T00:00:00Z --config {
	authentication: { users: { providers: { insecure: true } } },
	object: { ttt: 3600 },
	usage: true,
}
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

# Alice stores a file, so that the object's own touch at the same time is skipped.
let referent = tg --token $alice.token put 'tg.file("hello")' | str trim
tg index
let alice_usage = tg --token $alice.token user usage | from json

# Eve touches the file with Alice's token at the same time.
tg --token $eve.token object touch $referent
tg index

# Eve is charged for the same objects as Alice.
let eve_usage = tg --token $eve.token user usage | from json
assert ($eve_usage.object_count > 0) "a touch within the ttt must still charge the toucher"
assert equal $eve_usage.object_count $alice_usage.object_count
assert equal $eve_usage.object_size $alice_usage.object_size
