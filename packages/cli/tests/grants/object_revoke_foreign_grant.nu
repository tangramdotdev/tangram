use ../../test.nu *

# A principal who can read an object cannot revoke a grant another principal created on it; only the creator may revoke.

let remote = spawn --cloud --name remote --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg --url $remote.url login --verbose alice | from json
let bob = tg --url $remote.url login --verbose bob | from json
let eve = tg --url $remote.url login --verbose eve | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice stores a private file and grants both Bob and Eve the subtree.
let directory = tg --url $alice_local.url put 'tg.directory({ "hello.txt": tg.file("hello") })' | str trim
tg --url $alice_local.url index
let file = tg --url $alice_local.url children $directory | from json | get 0
tg --url $alice_local.url push --lazy $file
tg --url $remote.url index
tg --url $remote.url --token $alice.token grant $bob.user.id object_subtree $file | ignore
tg --url $remote.url --token $alice.token grant $eve.user.id object_subtree $file | ignore

# Eve can read the file, but she did not create Bob's grant, so she cannot revoke it.
let output = tg --url $remote.url --token $eve.token revoke $bob.user.id object_subtree $file | complete
failure $output "Eve should not revoke a grant she did not create."

# Bob's grant survives, so he retains access.
let bob_read = tg --url $remote.url --token $bob.token get $file | complete
success $bob_read "Bob should retain access after Eve's failed revoke."
