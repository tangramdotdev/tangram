use ../../util/test.nu *

# A destroy of a stopped sandbox on a remote runner records no error.

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
let url = $remote.url

let sandbox = tg --url $url --token $root_token sandbox create --wait --no-tokens | referent node
tg --url $url --token $root_token sandbox stop --wait $sandbox
tg --url $url --token $root_token sandbox destroy --wait $sandbox
let data = tg --url $url --token $root_token sandbox get $sandbox | from json | get data
assert equal $data.status destroyed "the sandbox should be destroyed"
assert equal ($data.error? | to json --raw) "null" "the destroyed sandbox should have no error"
