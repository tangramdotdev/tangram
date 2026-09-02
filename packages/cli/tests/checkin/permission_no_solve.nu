use ../../test.nu *
use ../lib/checkin.nu checkin-output

# A dependency grant that is not embedded in its reference is not observed with solving disabled.

let server = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	remotes: {}
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

let dependency = tg --token $alice.token put 'tg.directory({ "value": tg.file("no solve") })' | str trim
tg --token $alice.token index
tg --token $alice.token grant $bob.user.id object_subtree $dependency | ignore

let dependencies = [$dependency] | to json
let directory = artifact {
	input: (file --xattrs { "user.tangram.dependencies": $dependencies } no-solve)
}
let path = $directory | path join input
let output = checkin-output $server $path --no-solve --token $bob.token
assert equal $output.permissions [object_node] "the checkin token should confer only node permission"
