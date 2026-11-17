use ../../util/test.nu *

# Waiting with --until started for a queued sandbox prints its statuses and returns once the sandbox starts.

let local = server spawn --config {
	advanced: { checkpoints: true },
	runner: { cpus: 1, overcommit: { cpu: 1 } },
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
}

# Fill the runner, so that the sandbox waits in the queue.
let filler = tg sandbox create --wait --no-tokens --cpu 1 | referent node
let sandbox = tg sandbox create --no-tokens --cpu 1 | referent node

# Wait for the sandbox to start, and let the request reach the server before the runner is freed.
let subscribed = checkpoint watch sandbox.status.subscribed --params { sandbox: $sandbox }
let status = job spawn {
	let id = job id
	tg sandbox status --until started $sandbox | complete | job send --tag $id 0
}
checkpoint pass $subscribed
checkpoint unwatch $subscribed
tg sandbox destroy $filler

let output = job recv --tag $status --timeout 60sec
success $output "waiting for the sandbox to start should succeed"
assert equal ($output.stdout | from json) [starting started] "the statuses should end once the sandbox starts"
tg sandbox destroy $sandbox
