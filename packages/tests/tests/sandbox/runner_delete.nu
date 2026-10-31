use ../../util/test.nu *

# Deleting a runner destroys its stopped sandboxes, because no runner can report them anymore.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}

let sandbox = tg --url $remote.url --token $root_token sandbox create --no-tokens | referent node
tg --url $remote.url --token $root_token sandbox stop $sandbox
tg --url $remote.url --token $root_token runner delete $created.data.id

assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) destroyed
