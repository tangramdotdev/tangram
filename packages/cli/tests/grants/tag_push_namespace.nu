use ../../test.nu *

# Pushing a tag under a user requires write permission on that user.

let remote = spawn --cloud --name remote --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg --url $remote.url login --verbose --name alice | from json
let bob = tg --url $remote.url login --verbose --name bob | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}
tg --url $alice_local.url pull $alice.user.id
let alice_file = tg --url $alice_local.url put 'tg.file("alice")' | str trim
tg --url $alice_local.url tag put alice/allowed $alice_file
tg --url $alice_local.url push alice/allowed
success (tg --url $remote.url --token $alice.token tag get alice/allowed | complete)

let bob_local = spawn --name bob-local --config {
	remotes: {
		destination: { url: $remote.url, token: $bob.token },
		source: { url: $remote.url, token: $alice.token },
	},
}
tg --url $bob_local.url pull --remote=source $alice.user.id
let bob_file = tg --url $bob_local.url put 'tg.file("bob")' | str trim
tg --url $bob_local.url tag put alice/denied $bob_file
let output = tg --url $bob_local.url push --remote=destination alice/denied | complete
failure $output "Bob should not be able to push a tag under Alice's user."

tg --url $bob_local.url pull --remote=source alice/allowed
tg --url $bob_local.url tag put --force alice/allowed $bob_file
let output = tg --url $bob_local.url push --force --remote=destination alice/allowed | complete
failure $output "Bob should not be able to replace Alice's existing tag."
