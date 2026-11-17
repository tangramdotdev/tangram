use ../../util/test.nu *

# Waiting with --until fails when the timeout passes before the process has the status.

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
let filler = tg sandbox create --wait --no-tokens --cpu 1 | referent node
let spawn = tg process spawn --no-tokens --verbose --sandbox=true $command | from json
let process = $spawn.process

let output = tg process status --until started --timeout 500ms $process | complete
failure $output "waiting past the timeout should fail"
assert ($output.stderr | str contains "the timeout passed before the process had the status")
assert equal ($output.stdout | from json) [queued] "the statuses should be the statuses observed before the timeout"

tg process cancel $process $spawn.lease
tg sandbox destroy $filler
