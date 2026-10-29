use ../../util/test.nu *

# A file's token authorizes the checkin of its dependencies when the dependency references have no tokens.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	remotes: {}
	vfs: false
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let dependency = tg --token $alice.token put --no-tokens 'tg.directory({"library":tg.file("contents")})' | referent node
let source = 'tg.file({"contents":"wrapper","dependencies":{"DEPENDENCY":{"node":DEPENDENCY}}})'
	| str replace --all DEPENDENCY $dependency
let id = tg --token $alice.token put --no-tokens $source | referent node
tg index
let checkout = tg --token $alice.token checkout $id | str trim

# Copy the wrapper with its token but without the dependency tokens.
let path = (mktemp --directory) | path join wrapper
open --raw $checkout | save $path
xattr_write user.tangram.token (xattr_read user.tangram.token $checkout) $path
xattr_write user.tangram.dependencies ([$dependency] | to json --raw) $path

# Bob cannot read the dependency without a token.
let output = tg --token $bob.token get $dependency | complete
failure $output 'reading the dependency without a token should fail'

# Checkin authorizes the dependency with the file token.
let output = tg --token $bob.token checkin --no-tokens $path | complete
success $output 'the file token should authorize the dependency'
assert equal ($output.stdout | referent node) $id
