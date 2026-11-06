use ../../util/test.nu *

# A destroy during an in-flight create waits for the attempt, and then tears down what the runner created.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}
wait_for_log $runner 'the runner is ready to accept work'

# Hold the sandbox's connection, so that its scheduler still owns it.
let connect = checkpoint watch sandbox.control.connect --token $root_token --url $remote.url
let sandbox = tg --url $remote.url --token $root_token sandbox create --no-tokens | referent node
checkpoint wait $connect | ignore

# The destroy returns without the connection, and the teardown finishes once the connection is released.
let url = $remote.url
let destroy = job spawn {
	let id = job id
	tg --url $url --token $root_token sandbox destroy --wait $sandbox | complete | job send --tag $id 0
}
sleep 200ms
checkpoint unwatch $connect
success (job recv --tag $destroy --timeout 30sec) "the destroy should finish"
assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) destroyed
