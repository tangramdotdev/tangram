use ../../test.nu *

let server = server spawn --config {
	runner: { cpus: 1 },
}

let path = artifact {
	tangram.ts: '
		export async function blocker() {
			await tg.sleep(12);
		}
	',
}

tg build --detach $"($path)#blocker" | ignore
let output = tg sandbox create | complete
success $output "a queued sandbox should wait for runner capacity"
tg sandbox destroy ($output.stdout | str trim)
