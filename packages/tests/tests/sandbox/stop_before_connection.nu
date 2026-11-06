use ../../util/test.nu *

# A stop of a queued sandbox waits for the sandbox to start, and then stops it.

let local = server spawn --config {
	runner: { cpus: 1, overcommit: { cpu: 1 } },
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
}

# Fill the runner, so that the next sandbox waits in the queue.
let running = tg sandbox create --wait --no-tokens --cpu 1 | referent node
let queued = tg sandbox create --no-tokens --cpu 1 | referent node
assert equal (tg sandbox get $queued | from json | get data.status) starting

# The stop does not finish while the sandbox is queued.
let url = $local.url
let stop = job spawn {
	let id = job id
	tg --url $url sandbox stop --wait $queued | complete | job send --tag $id 0
}
assert equal (try { job recv --tag $stop --timeout 500ms } catch { null }) null

# Releasing the capacity lets the sandbox start, and the stop then stops it.
tg sandbox destroy --wait $running
success (job recv --tag $stop --timeout 30sec) "the stop should finish once the sandbox starts"
assert equal (tg sandbox get $queued | from json | get data.status) stopped
