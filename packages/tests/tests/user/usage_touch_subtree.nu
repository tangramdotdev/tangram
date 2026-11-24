use ../../util/test.nu *

# Touching another user's object with a token that grants its subtree charges the toucher for the whole subtree.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } },
	object: { ttt: 0 },
	usage: true,
}
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

# Alice stores a directory with a file and shares its referent with Eve.
let referent = tg --token $alice.token put 'tg.directory({ "file": tg.file("hello") })' | str trim
tg index
let alice_usage = tg --token $alice.token user usage | from json
assert ($alice_usage.object_count > 1) "the directory should have children"

# Eve touches the directory with Alice's token.
tg --token $eve.token object touch $referent
tg index

# Eve is charged for the same objects as Alice.
let eve_usage = tg --token $eve.token user usage | from json
assert equal $eve_usage.object_count $alice_usage.object_count
assert equal $eve_usage.object_size $alice_usage.object_size
