use ../../lib/test.nu *

# A scheduler that stops emitting heartbeats is treated as dead while a sandbox waits for capacity.

let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true },
	runner: { cpus: 1, overcommit: { cpu: 1 } },
	scheduler: {
		default_cpu: 1,
		default_memory: 1_073_741_824,
		heartbeat_interval: 30,
		heartbeat_ttl: 0.5,
	},
}

let path = artifact {
	tangram.ts: '
		export async function blocker() {
			await tg.sleep(60);
		}
	',
}

# Start a process that holds the runner's only capacity.
let watch = tg checkpoint watch runner.process.start | from json | get watch
tg build --no-tokens --detach $"($path)#blocker" | ignore
tg checkpoint wait runner.process.start $watch 0 | ignore
tg checkpoint continue runner.process.start $watch 0
tg checkpoint unwatch runner.process.start $watch

# The queued sandbox fails once the scheduler heartbeat expires.
let ttl = checkpoint watch clock.sleep --params { duration: '500ms' }
let create = job spawn {
	let id = job id
	tg sandbox create --no-tokens | complete | job send --tag $id 0
}
checkpoint pass $ttl | ignore
checkpoint unwatch $ttl
advance_time $local 1sec
let output = job recv --tag $create --timeout 30sec
failure $output "a queued sandbox should fail when its scheduler stops emitting heartbeats"
let stderr = $output.stderr | ansi strip
assert ($stderr | str contains "the scheduler heartbeat expired") "expected a scheduler heartbeat expiration error"
