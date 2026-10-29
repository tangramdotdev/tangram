use ../../lib/test.nu *

# A queued sandbox connects after a runner becomes available.

let local = server spawn --config {
	advanced: { checkpoints: true },
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
	runner: { cpus: 1, overcommit: { cpu: 1 } },
}

let path = artifact {
	tangram.ts: '
		export async function blocker() {
			await tg.sleep(60);
		}
	',
}

# Start a blocker that holds the runner's only capacity until it is cancelled.
let start = checkpoint watch runner.process.start
let blocker = tg build --no-tokens --detach --verbose $"($path)#blocker" | from json
checkpoint pass $start
checkpoint unwatch $start

# Create a sandbox, which must be enqueued while the blocker runs.
let enqueued = checkpoint watch scheduler.sandbox.enqueued
let create = job spawn {
	let job_id = job id
	tg sandbox create --no-tokens | complete | job send --tag $job_id 0
}
checkpoint pass $enqueued
checkpoint unwatch $enqueued

# Cancel the blocker, which makes the sandbox runnable.
tg cancel $blocker.process $blocker.lease
let output = job recv --tag $create --timeout 60sec
success $output "a queued sandbox should wait for runner capacity"
tg sandbox destroy ($output.stdout | referent node)
