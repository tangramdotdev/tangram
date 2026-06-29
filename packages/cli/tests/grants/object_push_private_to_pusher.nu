use ../../test.nu *

# Pushing an object to a shared remote keeps it private to the pusher: a second authenticated user and an anonymous client must not read it without a grant.

let remote = spawn --cloud --name remote --config { authentication: { providers: { insecure: true } } }

let alice = tg --url $remote.url login --verbose alice | from json
let bob = tg --url $remote.url login --verbose bob | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice stores a private file on her local server and pushes it to the remote as herself.
let file = tg --url $alice_local.url put 'tg.file("topsecret")' | str trim
tg --url $alice_local.url index
tg --url $alice_local.url push $file
tg --url $remote.url index

# Alice can read her own pushed file on the remote.
let owner = tg --url $remote.url --token $alice.token get $file | complete
success $owner "Alice should read her own pushed file on the remote."

# Bob, another user on the remote, must not read it without a grant.
let bob_read = tg --url $remote.url --token $bob.token get $file | complete
failure $bob_read "Bob must not read Alice's pushed file without a grant."

# An anonymous client must not read it either.
let anon = tg --url $remote.url get $file | complete
failure $anon "an anonymous client must not read Alice's pushed file."
