use ../../test.nu *

# A denied ancestor probe preserves its specifier and falls back to local creation.

let remote = spawn --cloud --name remote --config {
	authentication: { users: { providers: { insecure: true } } }
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let bob = tg --url $remote.url login --verbose --name bob | from json
let remote_parent = tg --url $remote.url --token $alice.token group create private | from json
tg --url $remote.url index

let local = spawn --name local --config {
	remotes: { default: { token: $bob.token, url: $remote.url } }
}
let node = tg --url $local.url put 'tg.file("data")' | str trim
let output = tg --url $local.url tag put -p private/child/tag $node | complete
success $output
assert not ($output.stderr | str contains $remote_parent.id) "the remote ID must not be disclosed"

let local_parent = tg --url $local.url group get private | from json
assert not equal $local_parent.id $remote_parent.id
