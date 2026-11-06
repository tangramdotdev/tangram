use ../../util/test.nu *

# A stopped sandbox keeps its storage reserved, so a new sandbox cannot take it, the stopped sandbox can start again, and its destruction releases the storage.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let local = server spawn --root --config {
	advanced: { checkpoints: true },
	runner: {
		backend: { kind: builtin, storage_limits: true },
		storage: 67108864,
	},
}

# Create a sandbox that reserves most of the runner's storage, and stop it.
let first = tg sandbox create --no-tokens --storage-min 50331648 --storage 50331648 | referent node
tg sandbox stop --wait $first

# Wait for a heartbeat that reports the runner's capacity after the stop.
let heartbeat = checkpoint watch runner.control.heartbeat.received
checkpoint pass $heartbeat 0

# Create a second sandbox that needs the same storage, and wait for a heartbeat after it is queued, so that the scheduler places it if the storage is available. It waits, because the stopped sandbox keeps its storage reserved.
let enqueued = checkpoint watch scheduler.sandbox.enqueued
let second = job spawn {
	let job_id = job id
	tg sandbox create --wait --no-tokens --storage-min 50331648 --storage 50331648 | complete | job send --tag $job_id 0
}
checkpoint pass $enqueued
checkpoint unwatch $enqueued
checkpoint pass $heartbeat 1
checkpoint unwatch $heartbeat

# The stopped sandbox starts again on its reserved storage.
success (timeout 60s tg sandbox start --wait $first | complete) "the stopped sandbox should start on its reserved storage"
assert equal (tg sandbox get $first | from json | get data.status) started

# Destroying the first sandbox releases the storage, so the second sandbox is created.
tg sandbox destroy $first
let output = job recv --tag $second --timeout 60sec
success $output "the second sandbox should be created once the storage is released"
let second = $output.stdout | referent node
assert equal (tg sandbox get $second | from json | get data.status) started "the second sandbox should run on the released storage"
