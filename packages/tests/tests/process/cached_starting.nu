use ../../util/test.nu *

# A process that is starting counts as a cache hit, which adds the caller's lease, so that the process outlives the first caller's cancel.

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
let first = tg process spawn --no-tokens --verbose --sandbox=true $command | from json
assert equal (tg get $first.process | from json | get status) starting

# A second spawn of the same command is a cache hit with its own lease.
tg index
let second = tg process spawn --no-tokens --verbose --sandbox=true $command | from json
assert equal $second.process $first.process
assert equal $second.cached true
assert not equal $second.lease $first.lease

# Canceling the first lease leaves the process queued, and canceling the last lease finishes it.
tg process cancel $first.process $first.lease
assert equal (tg get $first.process | from json | get status) starting
tg process cancel $first.process $second.lease
let outcome = tg wait $first.process | from json
assert equal $outcome.exit 1
tg sandbox destroy $sandbox
