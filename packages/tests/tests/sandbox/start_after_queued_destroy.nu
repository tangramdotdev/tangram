use ../../util/test.nu *

# A start that follows the destroy of a queued boot does not start the sandbox, although the index has not recorded the destruction yet.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { cpus: 1, id: $created.data.id, overcommit: { cpu: 1 }, remote: "default", token: $created.token.token },
}
let url = $remote.url

# Stop a sandbox, fill the runner with another sandbox, and start the first one, so that its next boot is queued.
let sandbox = tg --url $url --token $root_token sandbox create --wait --no-tokens --cpu 1 | referent node
tg --url $url --token $root_token sandbox stop --wait $sandbox
let running = tg --url $url --token $root_token sandbox create --wait --no-tokens --cpu 1 | referent node
success (timeout 30s tg --url $url --token $root_token sandbox start $sandbox | complete) "the start should return on acceptance"

# Hold the index writes, and destroy the queued boot, so that the index has not recorded the destruction.
let write = checkpoint watch index.write --token $root_token --url $url
let destroy = job spawn {
	let id = job id
	timeout 30s tg --url $url --token $root_token sandbox destroy $sandbox | complete | job send --tag $id 0
}
sleep 1sec

# Start the sandbox while the index still records the destroyed boot as starting.
let start = job spawn {
	let id = job id
	timeout 30s tg --url $url --token $root_token sandbox start $sandbox | complete | job send --tag $id 0
}
sleep 1sec
checkpoint unwatch $write
success (job recv --tag $destroy --timeout 30sec) "the destroy should succeed"
job recv --tag $start --timeout 30sec | ignore

# Free the runner. The sandbox stays destroyed, and the runner has capacity for another sandbox.
tg --url $url --token $root_token sandbox destroy --wait $running
sleep 2sec
assert equal (tg --url $url --token $root_token sandbox get $sandbox | from json | get data.status) destroyed "the sandbox should be destroyed"
success (timeout 30s tg --url $url --token $root_token sandbox create --wait --no-tokens --cpu 1 | complete) "the runner should have capacity for another sandbox"
