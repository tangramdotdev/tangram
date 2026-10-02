use ../lib/test.nu *
use ../lib/checkin.nu checkin-output

# Solving fails when an explicit dependency is inaccessible. Without solving, it remains opaque.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	remotes: {}
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

let dependency = tg --token $alice.token put --no-tokens 'tg.directory({ "value": tg.file("private") })' | referent node
tg --token $alice.token index

let dependencies = [$dependency] | to json
let directory = artifact {
	input: (file --xattrs { "user.tangram.dependencies": $dependencies } denied)
}
let path = $directory | path join input
let failed = try {
	checkin-output $local $path --token $bob.token | ignore
	false
} catch {
	true
}
assert $failed "the checkin should fail when the dependency is inaccessible"
let output = checkin-output $local $path --no-solve --token $bob.token
assert equal $output.permissions [object_node] "the checkin token should confer only node permission"
