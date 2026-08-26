use ../../test.nu *

# Only Root can mark a remote as trusted, and a later ordinary put clears the trust bit.

let root_token = random chars
let upstream = server spawn --name upstream
let server = server spawn --name server --config {
	authentication: {
		root: { token: $root_token },
		users: { providers: { insecure: true } },
	},
}
let alice = tg --url $server.url login --verbose --name alice | from json

let output = tg --url $server.url --token $alice.token remote put default $upstream.url --trusted | complete
failure $output "a user must not mark a remote as trusted"

tg --url $server.url --token $root_token remote put default $upstream.url --trusted
let remote = tg --url $server.url --token $root_token remote get default | from json
assert equal $remote.trusted true "Root should be able to mark a remote as trusted"

tg --url $server.url --token $root_token remote put default $upstream.url
let remote = tg --url $server.url --token $root_token remote get default | from json
assert equal $remote.trusted false "an ordinary put should clear the trust bit"
