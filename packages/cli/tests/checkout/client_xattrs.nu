use ../../test.nu *

# The client reads checkout metadata and writes the same schema for checkin.

const repository_path = path self '../../../..'

let server = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	remotes: {}
	vfs: false
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let dependency = tg --token $alice.token put 'tg.directory({"library":tg.file("contents")})' | str trim
let source = 'tg.file({"contents":"wrapper","dependencies":{"DEPENDENCY":{"node":DEPENDENCY}}})'
	| str replace --all DEPENDENCY $dependency
let id = tg --token $alice.token put $source | str trim
tg index
let path = tg --token $alice.token checkout $id | str trim
let fixture = {
	copy: ((mktemp --directory) | path join wrapper)
	dependency: $dependency
	id: $id
	path: $path
}
let output = with-env { TANGRAM_TEST_XATTRS: ($fixture | to json), TANGRAM_TOKEN: $bob.token } {
	cargo test --quiet --manifest-path ($repository_path | path join Cargo.toml) --package tangram_client --test xattrs -- --ignored | complete
}
success $output 'the client xattr API should interoperate with checkout and checkin'
