use ../../util/test.nu *

# A start that follows the stop of a queued boot starts the sandbox, although the index has not recorded the stop yet.

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

# Hold the index writes, and stop the queued boot, so that the index has not recorded the stop.
let write = checkpoint watch index.write --token $root_token --url $url
let stop = job spawn {
	let id = job id
	timeout 30s tg --url $url --token $root_token sandbox stop $sandbox | complete | job send --tag $id 0
}
sleep 1sec

# Start the sandbox again while the index still records the stopped boot as starting.
let start = job spawn {
	let id = job id
	timeout 30s tg --url $url --token $root_token sandbox start $sandbox | complete | job send --tag $id 0
}
sleep 1sec
checkpoint unwatch $write
success (job recv --tag $stop --timeout 30sec) "the stop should succeed"
success (job recv --tag $start --timeout 30sec) "the start should succeed"

# Free the runner. The sandbox starts, and a get reports it started.
tg --url $url --token $root_token sandbox destroy --wait $running
success (timeout 30s tg --url $url --token $root_token sandbox start --wait $sandbox | complete) "the sandbox should start"
assert equal (tg --url $url --token $root_token sandbox get $sandbox | from json | get data.status) started "the sandbox should be started"
