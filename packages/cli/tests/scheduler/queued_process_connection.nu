use ../lib/test.nu *

# A queued process connects after the scheduler makes it runnable.

let local = server spawn --config {
	runner: { cpus: 1 },
}

let path = artifact {
	tangram.ts: '
		export async function blocker() {
			await tg.sleep(12);
		}

		export function child() {
			return "child";
		}
	',
}

tg build --no-tokens --detach $"($path)#blocker" | ignore
let output = tg build $"($path)#child" | complete
success $output "a queued process should wait for runner capacity"
