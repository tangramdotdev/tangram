use ../../util/test.nu *

# A stop that arrives while the scheduler has offered the sandbox to a runner, and the runner has not accepted it yet, still stops the sandbox once the runner holds it.

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

# Hold the runner before it accepts the sandbox, so that the scheduler has offered the sandbox and has no reply.
let create = checkpoint watch runner.control.create_sandbox --url $runner.url
let sandbox = tg --url $remote.url --token $root_token sandbox create --no-tokens | referent node
checkpoint wait $create | ignore

# Stop the sandbox, and wait for the scheduler to reply that it does not hold the sandbox.
let not_holder = checkpoint watch sandbox.control.response.publish --params { kind: not_holder, sandbox: $sandbox } --token $root_token --url $remote.url
let url = $remote.url
let stop = job spawn {
	let id = job id
	timeout 30s tg --url $url --token $root_token sandbox stop --wait $sandbox | complete | job send --tag $id 0
}
checkpoint wait $not_holder | ignore
checkpoint unwatch $not_holder

# Let the runner accept the sandbox. The stop reaches the runner once the sandbox connects.
checkpoint unwatch $create
let output = job recv --tag $stop --timeout 30sec
success $output "the stop should succeed after the runner accepts the sandbox"
assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) stopped "the sandbox should be stopped"
