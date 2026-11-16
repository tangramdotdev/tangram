use ../../util/test.nu *

# A start's enqueue that is sent again after a destroy ended the queued sandbox and its scheduler forgot it does not leave the sandbox running, so the sandbox stays destroyed and the runner has capacity for another sandbox.

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

# Create a sandbox, and stop it.
let sandbox = tg --url $url --token $root_token sandbox create --wait --no-tokens --cpu 1 | referent node
tg --url $url --token $root_token sandbox stop --wait $sandbox

# Fill the runner with another sandbox, and start the first one, so that its start is queued.
let running = tg --url $url --token $root_token sandbox create --wait --no-tokens --cpu 1 | referent node
success (timeout 30s tg --url $url --token $root_token sandbox start $sandbox | complete) "the start should return on acceptance"

# Start the sandbox again, which sends the queued start's enqueue again, and hold that enqueue.
let resume = checkpoint watch sandbox.start.resume --params { sandbox: $sandbox } --token $root_token --url $url
timeout 30s tg --url $url --token $root_token sandbox start $sandbox | complete | ignore
checkpoint wait $resume | ignore

# Destroy the queued sandbox, and await indexing, so that the scheduler records the destruction and forgets the sandbox.
success (timeout 30s tg --url $url --token $root_token sandbox destroy --wait $sandbox | complete) "the destroy should succeed"
tg --url $url --token $root_token index

# Let the enqueue reach the scheduler, which queues the sandbox again.
let enqueued = checkpoint watch scheduler.sandbox.enqueued --params { sandbox: $sandbox } --token $root_token --url $url
checkpoint continue $resume
checkpoint pass $enqueued | ignore
checkpoint unwatch $enqueued
checkpoint unwatch $resume

# Free the runner, so that the runner starts the queued sandbox and connects it.
let connect = checkpoint watch sandbox.control.index.submitted --params { sandbox: $sandbox } --token $root_token --url $url
tg --url $url --token $root_token sandbox destroy --wait $running
checkpoint pass $connect | ignore
checkpoint unwatch $connect

# The sandbox is destroyed, and the runner has capacity for another sandbox.
assert equal (tg --url $url --token $root_token sandbox get $sandbox | from json | get data.status) destroyed "the sandbox should be destroyed"
success (timeout 30s tg --url $url --token $root_token sandbox create --wait --no-tokens --cpu 1 | complete) "the runner should have capacity for another sandbox"
