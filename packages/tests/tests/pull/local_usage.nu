use ../../util/test.nu *

# A pull of another user's stored object with a token that grants its subtree takes the local path and charges the puller for the whole subtree.

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

# Eve pulls the directory with Alice's token, which takes the local path.
let output = tg --no-quiet --token $eve.token pull $referent | complete
success $output
assert not ($output.stderr | str contains 'tokens[') "the pull should not start a sync"
tg index

# Eve is charged for the same objects as Alice.
let eve_usage = tg --token $eve.token user usage | from json
assert equal $eve_usage.object_count $alice_usage.object_count
assert equal $eve_usage.object_size $alice_usage.object_size
