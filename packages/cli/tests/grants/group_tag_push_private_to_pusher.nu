use ../../test.nu *

# Pushing a group and tag to a shared remote grants the pusher access without exposing them to other users.

let remote = spawn --cloud --name remote --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg --url $remote.url login --verbose --name alice | from json
let bob = tg --url $remote.url login --verbose --name bob | from json
let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

tg --url $alice_local.url group create private
let file = tg --url $alice_local.url put 'tg.file("secret")' | str trim
tg --url $alice_local.url tag put private/1.0.0 $file
tg --url $alice_local.url push --group-children private
tg --url $remote.url index

success (tg --url $remote.url --token $alice.token group get private | complete)
success (tg --url $remote.url --token $alice.token tag get private/1.0.0 | complete)
let grants = tg --url $remote.url --token $alice.token grants list --resource private | from json
assert (($grants | length) == 0) "pushing should not persist grants for groups"
failure (tg --url $remote.url --token $bob.token group get private | complete)
failure (tg --url $remote.url --token $bob.token tag get private/1.0.0 | complete)
failure (tg --url $remote.url group get private | complete)
failure (tg --url $remote.url tag get private/1.0.0 | complete)
