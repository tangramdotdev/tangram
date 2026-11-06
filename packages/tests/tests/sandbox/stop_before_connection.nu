use ../../util/test.nu *

# A stop of a queued sandbox returns once it is accepted, and the sandbox is stopped as soon as it starts.

let local = server spawn --config {
	runner: { cpus: 1, overcommit: { cpu: 1 } },
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
}

# Fill the runner, so that the next sandbox waits in the queue.
let running = tg sandbox create --wait --no-tokens --cpu 1 | referent node
let queued = tg sandbox create --no-tokens --cpu 1 | referent node
assert equal (tg sandbox get $queued | from json | get data.status) starting

# The stop returns while the sandbox is queued, and a repeated stop is also accepted.
success (tg sandbox stop $queued | complete) "the stop should return on acceptance"
assert equal (tg sandbox get $queued | from json | get data.status) starting
success (tg sandbox stop $queued | complete) "a repeated stop should be accepted"

# A stop with a wait does not finish while the sandbox is queued.
let url = $local.url
let stop = job spawn {
	let id = job id
	tg --url $url sandbox stop --wait $queued | complete | job send --tag $id 0
}
assert equal (try { job recv --tag $stop --timeout 500ms } catch { null }) null

# Releasing the capacity lets the sandbox start, and the sandbox is then stopped.
tg sandbox destroy --wait $running
success (job recv --tag $stop --timeout 30sec) "the stop should finish once the sandbox stops"
assert equal (tg sandbox get $queued | from json | get data.status) stopped
