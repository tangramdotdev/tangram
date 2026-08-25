use ../../test.nu *

# Pushing a group under a user requires write permission on that user.

let remote = server spawn --cloud --name remote --config {
	authentication: { users: { providers: { insecure: true } } }
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let bob = tg --url $remote.url login --verbose --name bob | from json

let alice_local = server spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}
tg --url $alice_local.url pull $alice.user.id
tg --url $alice_local.url group create alice/allowed | ignore
tg --url $alice_local.url push alice/allowed
success (tg --url $remote.url --token $alice.token group get alice/allowed | complete)

let bob_local = server spawn --name bob-local --config {
	remotes: {
		destination: { token: $bob.token, url: $remote.url },
		source: { token: $alice.token, url: $remote.url },
	},
}
tg --url $bob_local.url pull --remote=source $alice.user.id
tg --url $bob_local.url group create alice/denied | ignore
let output = tg --url $bob_local.url push --remote=destination alice/denied | complete
failure $output "Bob should not be able to push a group under Alice's user."
assert ($output.stderr | str contains "unauthorized")
failure (tg --url $remote.url --token $alice.token group get alice/denied | complete)
