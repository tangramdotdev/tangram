use ../../test.nu *

# Reproduce checkin ignoring a valid token xattr on a path in the checkouts directory.

let server = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	remotes: {}
	vfs: false
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

let id = tg --token $alice.token put 'tg.file("contents")' | str trim
tg index
let path = tg --token $alice.token checkout $id | str trim
assert equal $path ($server.checkout_directory | path join $id)

# Bob needs the token to read Alice's private file.
let output = tg --token $bob.token get --bytes $id | complete
failure $output 'reading without the token should fail'
let token = xattr_read user.tangram.token $path
assert (not ($token | is-empty)) 'the checkout should have a file token xattr'
let body = $token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
assert equal $body.resource $id
assert equal $body.permissions [object_subtree]
let token = $token | url encode --all
let output = tg --token $bob.token get --bytes $'($id)?tokens[local]=($token)' | complete
success $output 'the token from the xattr should authorize the object read'

# Checkin should authorize the same path using the token in its xattr.
let output = tg --token $bob.token checkin $path | complete
success $output 'checkin should authorize using the file token xattr'
snapshot $output.stderr ''
assert equal ($output.stdout | str trim) $id
