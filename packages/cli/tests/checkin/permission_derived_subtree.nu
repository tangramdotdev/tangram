use ../lib/test.nu *
use ../lib/checkin.nu reference-access

# Permissions proven across a dependency's direct children make a checked-in file's reference grant another user read access to its subtree.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	remotes: {}
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let eve = tg login --verbose --name eve | from json

let dependency = tg --token $alice.token put --no-tokens 'tg.directory({ "value": tg.file("subtree") })' | referent node
tg --token $alice.token index
let child = tg --token $alice.token children $dependency | from json | first
tg --token $alice.token grant $bob.user.id object_node $dependency | ignore
tg --token $alice.token grant $bob.user.id object_subtree $child | ignore

let dependencies = [$dependency] | to json
let directory = artifact {
	input: (file --xattrs { "user.tangram.dependencies": $dependencies } subtree)
}
let path = $directory | path join input
let reference = tg --token $bob.token checkin --no-lock --root $path | str trim
assert equal (reference-access $reference --token $eve.token) subtree "the reference should grant the subtree"
