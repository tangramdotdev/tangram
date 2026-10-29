use ../../lib/test.nu *
use ../../lib/checkin.nu reference-access

# A dependency grant that is not embedded in its reference is not observed with solving disabled, so the checked-in file's reference does not grant another user access to the dependency.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	remotes: {}
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let eve = tg login --verbose --name eve | from json

let dependency = tg --token $alice.token put --no-tokens 'tg.directory({ "value": tg.file("no solve") })' | referent node
tg --token $alice.token index
tg --token $alice.token grant $bob.user.id object_subtree $dependency | ignore

let dependencies = [$dependency] | to json
let directory = artifact {
	input: (file --xattrs { "user.tangram.dependencies": $dependencies } no-solve)
}
let path = $directory | path join input
let reference = tg --token $bob.token checkin --no-lock --root --no-solve $path | str trim
assert equal (reference-access $reference --token $eve.token) node "the reference should grant only the checked in file"
