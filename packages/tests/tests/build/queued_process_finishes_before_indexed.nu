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
let finish = tg checkpoint watch runner.process.finish | from json | get watch
tg build --detach --verbose $first | ignore
success (tg checkpoint wait --timeout 30s runner.process.finish $finish 0 | complete)

# Hold the index so that the second process does not reach it.
let batch = tg checkpoint watch index.batch | from json | get watch

# Start a build that must wait for capacity, and observe its wait on the index.
let start = tg checkpoint watch process.spawn.start.index | from json | get watch
let build = job spawn {
	let job_id = job id
	let output = tg build --no-tokens $second | complete
	$output | job send --tag $job_id 0
}
success (tg checkpoint wait --timeout 30s process.spawn.start.index $start 0 | complete)
tg checkpoint unwatch process.spawn.start.index $start

# Release the runner so that the second process runs and finishes while the index is still held.
tg checkpoint unwatch runner.process.finish $finish
let held = try { job recv --tag $build --timeout 20sec } catch { null }
tg checkpoint unwatch index.batch $batch
let output = if $held != null { $held } else { job recv --tag $build --timeout 30sec }
success $output "a process that succeeded must not fail its build"
assert ($output.stdout | str contains "second")
