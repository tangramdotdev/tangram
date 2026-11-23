use ../../util/test.nu *

# A start that comes after a stop of a queued sandbox cancels the stop, so that the sandbox stays started.

let local = server spawn --config {
	advanced: { checkpoints: true },
	runner: { cpus: 1, overcommit: { cpu: 1 } },
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
}

# Fill the runner, so that the next sandbox waits in the queue.
let started = tg sandbox create --wait --no-tokens --cpu 1 | referent node
let queued = tg sandbox create --no-tokens --cpu 1 | referent node

# Stop the queued sandbox, then start it.
success (tg sandbox stop $queued | complete) "the stop should return on acceptance"
success (tg sandbox start $queued | complete) "the start should return on acceptance"

# Releasing the capacity lets the sandbox start, and once the runner has it ready without the stop, it stays started.
let ready = checkpoint watch runner.sandbox.ready --params { sandbox: $queued }
tg sandbox destroy --wait $started
checkpoint pass $ready | ignore
checkpoint unwatch $ready
success (timeout 30s tg sandbox status --until started $queued | complete) "the sandbox should start"
assert equal (tg sandbox get $queued | from json | get data.status) started
tg sandbox destroy $queued
