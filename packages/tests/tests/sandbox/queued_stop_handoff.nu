use ../../util/test.nu *

# A stop of a queued sandbox travels with its offer to the runner, so that the sandbox is stopped once it starts, unless a start sent while the scheduler hands the sandbox to the runner reaches the runner first, in which case the sandbox ends started.

let local = server spawn --config {
	advanced: { checkpoints: true },
	runner: { cpus: 1, overcommit: { cpu: 1 } },
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
}
let url = $local.url

# Queue a sandbox behind a full runner, stop it while it is queued, and then hold the runner before it accepts the offer of the sandbox.
def offered [] {
	let running = tg sandbox create --wait --no-tokens --cpu 1 | referent node
	let queued = tg sandbox create --no-tokens --cpu 1 | referent node
	success (tg sandbox stop $queued | complete) "the stop should return on acceptance"
	let create = checkpoint watch runner.control.create_sandbox --params { sandbox: $queued }
	tg sandbox destroy --wait $running
	checkpoint wait $create | ignore
	{ create: $create, sandbox: $queued }
}

# Start the sandbox during the handoff. It ends started.
let offered = offered
let start = job spawn {
	let id = job id
	timeout 30s tg --url $url sandbox start --wait $offered.sandbox | complete | job send --tag $id 0
}
sleep 500ms
checkpoint unwatch $offered.create
success (job recv --tag $start --timeout 30sec) "the start should succeed"
sleep 1sec
assert equal (tg sandbox get $offered.sandbox | from json | get data.status) started "the sandbox should stay started"
tg sandbox destroy --wait $offered.sandbox

# Without a start, the sandbox stops once it starts.
let offered = offered
checkpoint unwatch $offered.create
success (timeout 30s tg sandbox stop --wait $offered.sandbox | complete) "the stop should finish"
assert equal (tg sandbox get $offered.sandbox | from json | get data.status) stopped "the sandbox should be stopped"
assert equal (tg sandbox get $offered.sandbox | from json | get data.boot? | default 0) 0 "the sandbox should not have started again"
