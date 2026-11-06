use ../../util/test.nu *

# A request to a full queue fails the call, and the sandboxes that it already accepted are unaffected.

let local = server spawn --config {
	runner: { cpus: 1, overcommit: { cpu: 1 } },
	scheduler: { create_sandbox_queue_capacity: 1, default_cpu: 1, default_memory: 1_073_741_824 },
}
let running = tg sandbox create --wait --no-tokens --cpu 1 | referent node
let queued = tg sandbox create --no-tokens --cpu 1 | referent node
let output = tg sandbox create --no-tokens --cpu 1 | complete
failure $output "a request to a full queue should fail"
assert ($output.stderr | str contains "the scheduler queue is full")
assert equal (tg sandbox get $queued | from json | get data.status) starting
