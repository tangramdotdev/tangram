use ../../test.nu *
use ../lib/checkin.nu checkin-output

# Solving fails when node permission does not reveal whether an artifact's subtree needs solving.

let server = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	remotes: {}
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

let dependency = tg --token $alice.token put 'tg.directory({ "value": tg.file("node") })' | str trim
tg --token $alice.token index
tg --token $alice.token grant $bob.user.id object_node $dependency | ignore

let dependencies = [$dependency] | to json
let directory = artifact {
	input: (file --xattrs { "user.tangram.dependencies": $dependencies } node)
}
let path = $directory | path join input
let failed = try {
	checkin-output $server $path --token $bob.token | ignore
	false
} catch {
	true
}
assert $failed "the checkin should fail rather than treating unknown subtree metadata as opaque"
