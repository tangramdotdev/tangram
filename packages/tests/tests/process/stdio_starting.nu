use ../../util/test.nu *

# Requests that a spawner sends to a process that is still starting wait for it to start, however long it waits in the queue.

let local = server spawn --config {
	process: {
		stdio_relay: { ack_delay: 0.01, max_retransmits: 2, timeout: 0.1 },
		stdio_relay_retry: { max_retries: 1 },
	},
	runner: { cpus: 1, overcommit: { cpu: 1 } },
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
}
let mount = artifact {}
let path = artifact {
	tangram.ts: '
		export function hello() {
			return tg.command({
				args: ["-ec", "read line; echo \"hello $line\""],
				executable: "/bin/sh",
				host: tg.host.current,
			});
		}

		export function spin() {
			return tg.command({
				args: ["-ec", "while :; do :; done"],
				executable: "/bin/sh",
				host: tg.host.current,
			});
		}
	',
}
let hello = tg build $'($path)#hello' | str trim
let spin = tg build $'($path)#spin' | str trim

# Fill the runner, so that the processes wait in the queue, and free it after the relay's timeout has passed.
let sandbox = tg sandbox create --wait --no-tokens --cpu 1 | referent node
let free = job spawn {
	sleep 2sec
	tg sandbox destroy $sandbox
}

# Reads and writes wait for the process to start.
let output = "world\n" | tg run --no-tokens --cached=false --sandbox=true $hello | complete
success $output
assert ($output.stdout | str contains 'hello world')

# A signal waits for the process to start.
let sandbox = tg sandbox create --wait --no-tokens --cpu 1 | referent node
let spawned = tg process spawn --no-tokens --verbose --sandbox=true --mount $"($mount):/target" $spin | from json
assert equal (tg get $spawned.process | from json | get status) starting
assert equal (tg process children $spawned.process | from json) []
let free = job spawn {
	sleep 2sec
	tg sandbox destroy $sandbox
}
success (tg signal --signal KILL $spawned.process | complete)
let outcome = tg wait $spawned.process | from json
assert ($outcome.exit != 0)

# A read of a process that finishes without starting ends.
let sandbox = tg sandbox create --wait --no-tokens --cpu 1 | referent node
let run = job spawn {
	let job_id = job id
	let output = "world\n" | tg run --no-tokens --cached=false --sandbox=true $hello | complete
	$output | job send --tag $job_id 0
}
sleep 1sec
let queued = tg process list --status starting | from json | first | get node
let queued = tg get $queued | from json | get sandbox
tg sandbox destroy $queued
let output = job recv --tag $run --timeout 30sec
assert ($output.exit_code != 0)
assert equal $output.stdout ''
tg sandbox destroy $sandbox
