use ../../util/test.nu *

# A process that a spawn hands to this server's runner is queued before the runner holds it, and a second spawn of the same command is a cache hit that gets its lease once the runner holds the process, so that both spawns return the one process.

let server = server spawn --config { advanced: { checkpoints: true } }
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

# Hold the first process before this server's runner holds it.
let insert = checkpoint watch runner.process.state.insert
let first = job spawn {
	let id = job id
	tg process spawn --no-tokens --verbose --sandbox=true $command | complete | job send --tag $id 0
}
checkpoint wait $insert 0 | ignore
tg index

# A second spawn of the same command finds the queued process, and asks for its lease before the runner holds it.
let lease = checkpoint watch process.control.request.sent --params { kind: acquire_lease }
let second = job spawn {
	let id = job id
	tg process spawn --no-tokens --verbose --sandbox=true $command | complete | job send --tag $id 0
}
checkpoint wait $lease 0 | ignore
checkpoint unwatch $lease

# Let the runner hold the first process, and every other process that it starts.
checkpoint unwatch $insert

# Both spawns return the one process.
let first = job recv --tag $first --timeout 30sec
success $first 'the first spawn should succeed'
let first = $first.stdout | from json
let second = job recv --tag $second --timeout 30sec
success $second 'the second spawn should succeed'
let second = $second.stdout | from json
assert equal $second.process $first.process 'the second spawn should get the first process'
assert equal $second.cached true 'the second spawn should be a cache hit'
let outcome = tg wait $first.process | from json
assert equal $outcome.exit 0
