use ../../util/test.nu *

# A build whose process waited for capacity and finished successfully before reaching the index must succeed.

let local = server spawn --config {
	advanced: { checkpoints: true },
	runner: { cpus: 1, overcommit: { cpu: 1 } },
	scheduler: { default_cpu: 1 },
}
let first = artifact { tangram.ts: 'export default () => "first";' }
let second = artifact { tangram.ts: 'export default () => "second";' }
tg checkin $first | ignore
tg checkin $second | ignore
tg index

# Occupy the runner with a process that is held at its finish.
let finish = checkpoint watch runner.process.finish
tg build --detach $first | ignore
checkpoint wait $finish | ignore

# Hold the index so that the second process does not reach it.
let batch = checkpoint watch index.batch

# Start a build that must wait for capacity, and observe its wait on the index.
let start = checkpoint watch process.spawn.start.index
let ended = checkpoint watch process.spawn.sandbox.ended
let build = job spawn {
	let job_id = job id
	let output = tg build --no-tokens $second | complete
	$output | job send --tag $job_id 0
}
checkpoint wait $start | ignore
checkpoint unwatch $start

# Release the runner so that the second process runs and finishes while the index is still held.
checkpoint unwatch $finish
checkpoint wait $ended | ignore

# Release the index after the spawn observes the sandbox ending without an indexed process status.
checkpoint unwatch $batch
checkpoint unwatch $ended
let output = job recv --tag $build --timeout 30sec
success $output "a process that succeeded must not fail its build"
assert equal ($output.stdout | str trim) '"second"'
