use ../../util/test.nu *

# A process spawned into a sandbox that is still queued runs once the sandbox starts, on the sandbox's status change rather than after the sender's backoff.

let local = server spawn --config {
	control: { request_retry: { backoff: 60, jitter: 0, max_delay: 60 } },
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

# Fill the runner, so that the sandbox waits in the queue.
let filler = tg sandbox create --wait --no-tokens --cpu 1 | referent node
let sandbox = tg sandbox create --no-tokens --cpu 1 | referent node
assert equal (tg sandbox get $sandbox | from json | get data.status) starting

# Spawn a process into the queued sandbox, and then free the runner.
let run = job spawn {
	let id = job id
	timeout 30s tg run --no-tokens $'--sandbox=($sandbox)' $command | complete | job send --tag $id 0
}
sleep 1sec
tg sandbox destroy $filler
let output = job recv --tag $run --timeout 30sec
success $output "the process should run once the sandbox starts"
assert ($output.stdout | str contains 'hello')
tg sandbox destroy $sandbox
