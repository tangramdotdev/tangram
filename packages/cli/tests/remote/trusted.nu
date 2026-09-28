use ../lib/test.nu *

# Only Root can mark a remote as trusted, and a later ordinary put clears the trust bit.

let root_token = random chars
let remote_upstream = server spawn --name remote-upstream
let local = server spawn --name local --config {
	authentication: {
		root: { token: $root_token },
		users: { providers: { insecure: true } },
	},
}
let alice = tg --url $local.url login --verbose --name alice | from json

let output = tg --url $local.url --token $alice.token remote put default $remote_upstream.url --trusted | complete
failure $output "a user must not mark a remote as trusted"

tg --url $local.url --token $root_token remote put default $remote_upstream.url --trusted
let remote = tg --url $local.url --token $root_token remote get default | from json
assert equal $remote.trusted true "Root should be able to mark a remote as trusted"

tg --url $local.url --token $root_token remote put default $remote_upstream.url
let remote = tg --url $local.url --token $root_token remote get default | from json
assert equal $remote.trusted false "an ordinary put should clear the trust bit"
