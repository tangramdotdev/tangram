use ../../util/test.nu *
use ../../util/checkin.nu reference-access

# Solving fails when an explicit dependency is inaccessible. Without solving, the reference grants another user read access to the checked-in file but not to the dependency.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	remotes: {}
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let eve = tg login --verbose --name eve | from json

let dependency = tg --token $alice.token put --no-tokens 'tg.directory({ "value": tg.file("private") })' | referent node
tg --token $alice.token index

let dependencies = [$dependency] | to json
let directory = artifact {
	input: (file --xattrs { "user.tangram.dependencies": $dependencies } denied)
}
let path = $directory | path join input
failure (tg --token $bob.token checkin --no-lock --root $path | complete) "the checkin should fail when the dependency is inaccessible"
let reference = tg --token $bob.token checkin --no-lock --root --no-solve $path | str trim
assert equal (reference-access $reference --token $eve.token) node "the reference should grant only the checked in file"
