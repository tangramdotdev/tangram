use ../../util/test.nu *

# Touching another user's object twice charges the toucher once.

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

# Eve touches the directory twice.
tg --token $eve.token object touch $referent
tg index
let first = tg --token $eve.token user usage | from json
tg --token $eve.token object touch $referent
tg index
let second = tg --token $eve.token user usage | from json

# Eve is charged once.
assert equal $first.object_count $alice_usage.object_count
assert equal $second.object_count $first.object_count
assert equal $second.object_size $first.object_size
