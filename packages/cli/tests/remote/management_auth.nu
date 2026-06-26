use ../../test.nu *

# Remote management requires authentication and each authenticated user manages their own isolated set of remotes.

let root_remote = spawn --name root-remote
let alice_server = spawn --name alice-remote
let bob_server = spawn --name bob-remote
let auth_enabled = spawn --config {
	authentication: { providers: { insecure: true } },
	remotes: { default: { url: $root_remote.url } },
} --name auth-enabled

let output = tg remote put default $alice_server.url | complete
failure $output "An unauthenticated request should not be able to manage remotes."
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to put the remote
	   name = default
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to put the remote
	   name = default
	-> unauthenticated

'

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

let alice_remotes = tg --token $alice.token remote list | from json
assert equal $alice_remotes []

tg --token $alice.token remote put default $alice_server.url
tg --token $bob.token remote put default $bob_server.url

let alice_remote = tg --token $alice.token remote get default | from json
assert equal $alice_remote.url $alice_server.url

let bob_remote = tg --token $bob.token remote get default | from json
assert equal $bob_remote.url $bob_server.url

tg --token $alice.token remote delete default
let alice_remotes = tg --token $alice.token remote list | from json
assert equal $alice_remotes []

let bob_remote = tg --token $bob.token remote get default | from json
assert equal $bob_remote.url $bob_server.url

let auth_disabled = spawn --name auth-disabled

tg remote put default $root_remote.url
tg remote delete default
