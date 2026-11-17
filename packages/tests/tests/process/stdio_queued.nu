use ../../util/test.nu *

# Requests that a spawner sends to a process that is still queued wait for it to start, however long it waits in the queue.

let local = server spawn --config {
	advanced: { checkpoints: true },
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

# Fill the runner, so that the process waits in the queue.
let sandbox = tg sandbox create --wait --no-tokens --cpu 1 | referent node

# Reads and writes wait for the process to start. Free the runner once the read and the write have reached the server and the relay's timeout, which is measured in real time, has passed several times over.
let read = checkpoint watch process.stdio.read.request
let write = checkpoint watch process.stdio.write.request
let run = job spawn {
	let job_id = job id
	let output = "world\n" | tg run --no-tokens --cached=false --sandbox=true $hello | complete
	$output | job send --tag $job_id 0
}
checkpoint pass $read | ignore
checkpoint unwatch $read
checkpoint pass $write | ignore
checkpoint unwatch $write

# Wait a real second, which is longer than the relay's peer timeout, because the relay measures its timeout in real time, so that the read and the write outlive it while the process is queued.
sleep 1sec
tg sandbox destroy $sandbox
let output = job recv --tag $run --timeout 30sec
success $output
assert ($output.stdout | str contains 'hello world')

# A signal waits for the process to start.
let sandbox = tg sandbox create --wait --no-tokens --cpu 1 | referent node
let spawned = tg process spawn --no-tokens --verbose --sandbox=true --mount $"($mount):/target" $spin | from json
assert equal (tg get $spawned.process | from json | get status) queued
assert equal (tg process children $spawned.process | from json) []
let not_ready = checkpoint watch process.control.response.publish --params { kind: not_ready, process: $spawned.process }
let signal = job spawn {
	let job_id = job id
	tg signal --signal KILL $spawned.process | complete | job send --tag $job_id 0
}
checkpoint pass $not_ready | ignore
checkpoint unwatch $not_ready
tg sandbox destroy $sandbox
success (job recv --tag $signal --timeout 30sec)
let outcome = tg wait $spawned.process | from json
assert ($outcome.exit != 0)

# A read of a process that finishes without starting ends.
let sandbox = tg sandbox create --wait --no-tokens --cpu 1 | referent node
let read = checkpoint watch process.stdio.read.request
let run = job spawn {
	let job_id = job id
	let output = "world\n" | tg run --no-tokens --cached=false --sandbox=true $hello | complete
	$output | job send --tag $job_id 0
}
let queued = checkpoint pass $read | get params.process
checkpoint unwatch $read
let queued = tg get $queued | from json | get sandbox
tg sandbox destroy $queued
let output = job recv --tag $run --timeout 30sec
assert ($output.exit_code != 0)
assert equal $output.stdout ''
tg sandbox destroy $sandbox
