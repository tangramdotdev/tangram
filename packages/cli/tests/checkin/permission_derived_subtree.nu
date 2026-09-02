use ../../test.nu *
use ../lib/checkin.nu checkin-output

# Permissions proven across a dependency's direct children make a checked-in file subtree-readable.

let server = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	remotes: {}
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

let dependency = tg --token $alice.token put 'tg.directory({ "value": tg.file("subtree") })' | str trim
tg --token $alice.token index
let child = tg --token $alice.token children $dependency | from json | first
tg --token $alice.token grant $bob.user.id object_node $dependency | ignore
tg --token $alice.token grant $bob.user.id object_subtree $child | ignore

let dependencies = [$dependency] | to json
let directory = artifact {
	input: (file --xattrs { "user.tangram.dependencies": $dependencies } subtree)
}
let path = $directory | path join input
let output = checkin-output $server $path --token $bob.token
assert equal $output.permissions [object_subtree] "the checkin token should confer subtree permission"
