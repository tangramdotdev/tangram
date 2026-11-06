use ../../util/test.nu *

# A start that comes after a stop of a queued sandbox cancels the stop, so that the sandbox stays started.

let local = server spawn --config {
	runner: { cpus: 1, overcommit: { cpu: 1 } },
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
}

# Fill the runner, so that the next sandbox waits in the queue.
let running = tg sandbox create --wait --no-tokens --cpu 1 | referent node
let queued = tg sandbox create --no-tokens --cpu 1 | referent node

# Stop the queued sandbox, then start it.
success (tg sandbox stop $queued | complete) "the stop should return on acceptance"
success (tg sandbox start $queued | complete) "the start should return on acceptance"

# Releasing the capacity lets the sandbox start, and it stays started.
tg sandbox destroy --wait $running
loop {
	if (tg sandbox get $queued | from json | get data.status) == started { break }
	sleep 100ms
}
sleep 2sec
assert equal (tg sandbox get $queued | from json | get data.status) started
tg sandbox destroy $queued
