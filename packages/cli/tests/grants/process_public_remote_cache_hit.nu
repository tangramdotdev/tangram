use ../../test.nu *

# Building --public in a multi-server setup is safe: it operates on the builder's own local process, not a remote one. Even when the remote holds another user's process for the same command, --public must succeed without attempting to grant on the remote's process.

let remote = server spawn --cloud --name remote --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg --url $remote.url login --verbose --name alice | from json
let bob = tg --url $remote.url login --verbose --name bob | from json

# Bob builds the command locally, pushes the process to the remote, and grants Alice read on it.
let bob_local = server spawn --name bob-local --config {
	remotes: { default: { url: $remote.url, token: $bob.token } },
}
let path = artifact { tangram.ts: 'export default function () { return tg.file("bobs"); }' }
let bob_build = tg --url $bob_local.url build --detach --verbose $path | from json
tg --url $bob_local.url wait $bob_build.process | complete
tg --url $bob_local.url push $bob_build.process
tg --url $remote.url --token $bob.token grant $alice.user.id process_node $bob_build.process | ignore

# Alice has a local server that uses the remote.
let local = server spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice builds --public locally; it must succeed without granting on the remote's process.
let public = tg --url $local.url build --detach --verbose --public $path | complete
success $public "building --public in a multi-server setup must succeed."
