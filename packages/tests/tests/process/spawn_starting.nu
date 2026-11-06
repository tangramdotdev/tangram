use ../../util/test.nu *

# A spawn returns once the scheduler accepts the process, whose record is visible, and which can be canceled before any runner connects.

let local = server spawn --config {
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
let sandbox = tg sandbox create --wait --no-tokens --cpu 1 | referent node
let output = tg process spawn --no-tokens --verbose --sandbox=true $command | from json
let process = $output.process
assert equal (tg get $process | from json | get status) starting

# Canceling the process releases its lease at the scheduler, which finishes it.
tg process cancel $process $output.lease
let outcome = tg wait $process | from json
assert equal $outcome.exit 1
assert equal (tg get $process | from json | get status) finished
tg sandbox destroy $sandbox
