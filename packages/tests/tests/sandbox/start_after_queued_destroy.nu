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

# Hold the index writes, and destroy the queued boot, so that the scheduler ends the boot while the index has not recorded the destruction.
let write = checkpoint watch index.write --token $root_token --url $url
let end = checkpoint watch scheduler.sandbox.end --params { sandbox: $sandbox, status: destroyed } --token $root_token --url $url
let destroy = job spawn {
	let id = job id
	timeout 30s tg --url $url --token $root_token sandbox destroy $sandbox | complete | job send --tag $id 0
}
checkpoint pass $end | ignore
checkpoint unwatch $end

# Hold the connection of any later boot of the sandbox, so that a boot that the scheduler started again would keep the runner's capacity.
let connect = checkpoint watch sandbox.control.connect --params { sandbox: $sandbox } --token $root_token --url $url

# Start the sandbox while the index still records the ended boot as starting, so that the start's request reaches the scheduler, which holds the ended boot, and let the scheduler answer the start before the index records the destruction.
let reply = checkpoint watch sandbox.control.response.publish --params { kind: start, sandbox: $sandbox } --token $root_token --url $url
let start = job spawn {
	let id = job id
	timeout 30s tg --url $url --token $root_token sandbox start $sandbox | complete | job send --tag $id 0
}
checkpoint pass $reply | ignore
checkpoint unwatch $reply
checkpoint unwatch $write
success (job recv --tag $destroy --timeout 30sec) "the destroy should succeed"
job recv --tag $start --timeout 30sec | ignore

# Free the runner. The runner has capacity for another sandbox, because the scheduler does not place the destroyed sandbox ahead of it, and the sandbox stays destroyed.
tg --url $url --token $root_token sandbox destroy --wait $running
success (timeout 10s tg --url $url --token $root_token sandbox create --wait --no-tokens --cpu 1 | complete) "the runner should have capacity for another sandbox"
checkpoint unwatch $connect
tg --url $url --token $root_token index
assert equal (tg --url $url --token $root_token sandbox get $sandbox | from json | get data.status) destroyed "the sandbox should be destroyed"
