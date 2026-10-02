use ../lib/test.nu *

# A queued sandbox connects after a runner becomes available.

let local = server spawn --config {
	runner: { cpus: 1 },
}

let path = artifact {
	tangram.ts: '
		export async function blocker() {
			await tg.sleep(12);
		}
	',
}

tg build --no-tokens --detach $"($path)#blocker" | ignore
let output = tg sandbox create --no-tokens | complete
success $output "a queued sandbox should wait for runner capacity"
tg sandbox destroy ($output.stdout | str trim)
