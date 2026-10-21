use ../lib/test.nu *

# A scheduler that stops emitting heartbeats is treated as dead while a sandbox waits for capacity.

let local = server spawn --config {
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
let start = date now
let output = tg sandbox create --no-tokens | complete
failure $output "a queued sandbox should fail when its scheduler stops emitting heartbeats"
assert ((date now) - $start < 5sec) "the sandbox create should fail after the heartbeat TTL"
let stderr = $output.stderr | ansi strip
assert ($stderr | str contains "the scheduler heartbeat expired") "expected a scheduler heartbeat expiration error"
