use ../../util/test.nu *

# Waiting with --until for a status that the process can no longer have fails, including waiting for a process that finished without starting to start.

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

# Fill the runner, so that the process waits in the queue, and cancel the process, which finishes it without starting.
let filler = tg sandbox create --wait --no-tokens --cpu 1 | referent node
let output = tg process spawn --no-tokens --verbose --sandbox=true $command | from json
let process = $output.process
tg process cancel $process $output.lease
tg wait $process | ignore

let output = tg process status --until started $process | complete
failure $output "waiting for a process that finished without starting to start should fail"
assert ($output.stderr | str contains "the process finished without starting")
assert equal ($output.stdout | from json) [finished]

let output = tg process status --until queued $process | complete
failure $output "waiting for a finished process to be queued should fail"
assert ($output.stderr | str contains "the process can no longer have the status")
assert equal ($output.stdout | from json) [finished]
tg sandbox destroy $filler
