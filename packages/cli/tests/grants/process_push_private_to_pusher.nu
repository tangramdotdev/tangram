use ../../test.nu *

# Pushing a process to a shared remote keeps it private to the pusher: a second authenticated user and an anonymous client must not read the process node without a grant.

let remote = spawn --cloud --name remote --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg --url $remote.url login --verbose alice | from json
let bob = tg --url $remote.url login --verbose bob | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice builds a private process and pushes it to the remote as herself.
let path = artifact { tangram.ts: 'export default function () { return tg.file("topsecret"); }' }
let process = tg --url $alice_local.url build --detach $path | str trim
tg --url $alice_local.url wait $process | complete
tg --url $alice_local.url index
tg --url $alice_local.url push $process
tg --url $remote.url index

# Alice can read her own pushed process on the remote.
let owner = tg --url $remote.url --token $alice.token get $process | complete
success $owner "Alice should read her own pushed process on the remote."

# Bob, another user on the remote, must not read it without a grant.
let bob_read = tg --url $remote.url --token $bob.token get $process | complete
failure $bob_read "Bob must not read Alice's pushed process without a grant."

# An anonymous client must not read it either.
let anon = tg --url $remote.url get $process | complete
failure $anon "an anonymous client must not read Alice's pushed process."
