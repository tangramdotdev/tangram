use ../../util/test.nu *

# A queued process connects after the scheduler makes it runnable.

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

		export function child() {
			return "child";
		}
	',
}

# Start a blocker that holds the runner's only capacity until it is cancelled.
let start = checkpoint watch runner.process.start
let blocker = tg build --no-tokens --detach --verbose $"($path)#blocker" | from json
checkpoint pass $start
checkpoint unwatch $start

# Build the child, which must be enqueued while the blocker runs.
let enqueued = checkpoint watch scheduler.sandbox.enqueued
let child = job spawn {
	let job_id = job id
	tg build $"($path)#child" | complete | job send --tag $job_id 0
}
checkpoint pass $enqueued
checkpoint unwatch $enqueued

# Cancel the blocker, which makes the child runnable.
tg cancel $blocker.process $blocker.lease
let output = job recv --tag $child --timeout 60sec
success $output "a queued process should wait for runner capacity"
