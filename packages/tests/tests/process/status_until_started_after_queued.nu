use ../../util/test.nu *

# Waiting with --until started for a queued process prints its statuses and returns once the process starts.

let local = server spawn --config {
	advanced: { checkpoints: true },
	runner: { cpus: 1, overcommit: { cpu: 1 } },
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
}
let path = artifact {
	tangram.ts: '
		export function hello() {
			return tg.command({
				args: ["-ec", "echo hello"],
				executable: "/bin/sh",
				host: tg.host.current,
			});
		}
	',
}
let command = tg build $'($path)#hello' | str trim

# Fill the runner, so that the process waits in the queue.
let filler = tg sandbox create --wait --no-tokens --cpu 1 | referent node
let process = tg process spawn --no-tokens --verbose --sandbox=true $command | from json | get process

# Hold the process before it finishes, so that it is started while the status is read.
let finish = checkpoint watch runner.process.finish --params { process: $process }

# Wait for the process to start, and let the request reach the server before the runner is freed.
let subscribed = checkpoint watch process.status.subscribed --params { process: $process }
let status = job spawn {
	let id = job id
	tg process status --until started $process | complete | job send --tag $id 0
}
checkpoint pass $subscribed
checkpoint unwatch $subscribed
tg sandbox destroy $filler

let output = job recv --tag $status --timeout 60sec
success $output "waiting for the process to start should succeed"
assert equal ($output.stdout | from json) [queued started] "the statuses should end once the process starts"

# Let the process finish.
checkpoint unwatch $finish
tg wait $process | ignore
