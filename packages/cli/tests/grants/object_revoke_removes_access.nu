use ../../test.nu *

# Revoking an object grant should succeed and remove the grantee's access.

let remote = spawn --cloud --name remote --config { authentication: { providers: { insecure: true } } }

let alice = tg --url $remote.url login --verbose alice | from json
let bob = tg --url $remote.url login --verbose bob | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice stores a private file on the remote and grants Bob the subtree.
let directory = tg --url $alice_local.url put 'tg.directory({ "hello.txt": tg.file("hello") })' | str trim
tg --url $alice_local.url index
let file = tg --url $alice_local.url children $directory | from json | get 0
tg --url $alice_local.url push --lazy $file
tg --url $remote.url index
tg --url $remote.url --token $alice.token grant $bob.user.id object_subtree $file | ignore

# Bob can read the file while the grant is in effect.
let granted = tg --url $remote.url --token $bob.token get $file | complete
success $granted "Bob should read the file while the grant is in effect."

# Revoking an object grant should succeed rather than fail because admin is invalid on an object resource.
let revoked = tg --url $remote.url --token $alice.token revoke $bob.user.id object_subtree $file | complete
success $revoked "revoking an object grant should succeed."

# After revocation Bob can no longer read the file.
let denied = tg --url $remote.url --token $bob.token get $file | complete
failure $denied "Bob should lose access after the grant is revoked."
