use ../lib/test.nu *

# Remote management requires authentication and each authenticated user manages their own isolated set of remotes.

let remote_root = server spawn --name remote-root
let remote_alice = server spawn --name remote-alice
let remote_bob = server spawn --name remote-bob
let local_auth_enabled = server spawn --config {
	authentication: { users: { providers: { insecure: true } } },
	remotes: { default: { url: $remote_root.url } },
} --name local-auth-enabled

let output = tg remote put default $remote_alice.url | complete
failure $output "An unauthenticated request should not be able to manage remotes."
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to put the remote
	   name = default
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to put the remote
	   name = default
	-> unauthenticated

'

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

let alice_remotes = tg --token $alice.token remote list | from json
assert equal $alice_remotes []

tg --token $alice.token remote put default $remote_alice.url
tg --token $bob.token remote put default $remote_bob.url

let alice_remote = tg --token $alice.token remote get default | from json
assert equal $alice_remote.url $remote_alice.url

let bob_remote = tg --token $bob.token remote get default | from json
assert equal $bob_remote.url $remote_bob.url

tg --token $alice.token remote delete default
let alice_remotes = tg --token $alice.token remote list | from json
assert equal $alice_remotes []

let bob_remote = tg --token $bob.token remote get default | from json
assert equal $bob_remote.url $remote_bob.url

let local_auth_disabled = server spawn --name local-auth-disabled

tg remote put default $remote_root.url
tg remote delete default
