use ../../test.nu *

# A scheduler that stops emitting heartbeats is treated as dead while a sandbox waits for capacity.

let server = server spawn --config {
	runner: { cpus: 1 },
	scheduler: {
		heartbeat_interval: 30,
		heartbeat_ttl: 3,
	},
}

let path = artifact {
	tangram.ts: '
		export async function blocker() {
			await tg.sleep(12);
		}
	',
}

tg build --detach $"($path)#blocker" | ignore
let start = date now
let output = tg sandbox create | complete
failure $output "a queued sandbox should fail when its scheduler stops emitting heartbeats"
assert ((date now) - $start < 6sec) "the sandbox create should fail after the heartbeat TTL"
let stderr = $output.stderr | ansi strip
assert ($stderr | str contains "the scheduler heartbeat expired") "expected a scheduler heartbeat expiration error"
