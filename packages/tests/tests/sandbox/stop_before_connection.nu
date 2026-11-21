use ../../util/test.nu *

# A stop of a queued sandbox ends the queued boot, because a stop never rides with an offer, so that the sandbox is stopped without starting and keeps no runner's capacity.

let local = server spawn --config {
	runner: { cpus: 1, overcommit: { cpu: 1 } },
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
}

# Fill the runner, so that the next sandbox waits in the queue.
let running = tg sandbox create --wait --no-tokens --cpu 1 | referent node
let queued = tg sandbox create --no-tokens --cpu 1 | referent node
assert equal (tg sandbox get $queued | from json | get data.status) starting

# The stop ends the queued boot, and a repeated stop is also accepted.
success (timeout 30s tg sandbox stop --wait $queued | complete) "the stop should end the queued boot"
assert equal (tg sandbox get $queued | from json | get data.status) stopped
success (tg sandbox stop $queued | complete) "a repeated stop should be accepted"

# Releasing the capacity does not start the stopped sandbox, so another sandbox gets the capacity.
tg sandbox destroy --wait $running
success (timeout 30s tg sandbox create --wait --no-tokens --cpu 1 | complete) "another sandbox should get the capacity"
assert equal (tg sandbox get $queued | from json | get data.status) stopped
