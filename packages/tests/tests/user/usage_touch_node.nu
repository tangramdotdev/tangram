use ../../util/test.nu *

# Touching another user's object with a permission for the object alone charges the toucher for the object alone.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } },
	object: { ttt: 0 },
	usage: true,
}
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

# Alice stores a directory with a file and grants Eve the node permission for the directory.
let id = tg --token $alice.token put --no-tokens 'tg.directory({ "file": tg.file("hello") })' | referent node
tg index
let size = tg --token $alice.token object get --bytes $id | into binary | bytes length
tg --token $alice.token grant $eve.user.id object_node $id | ignore

# Eve touches the directory.
tg --token $eve.token object touch $id
tg index

# Eve is charged for the directory and not for its children.
let eve_usage = tg --token $eve.token user usage | from json
assert equal $eve_usage.object_count 1
assert equal $eve_usage.object_size $size
