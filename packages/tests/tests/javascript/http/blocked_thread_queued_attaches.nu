use ../../../util/test.nu *

# Process attaches that are queued when the JavaScript thread blocks past the connection's idle timeout must succeed and spawn each process once.
let local = server spawn --config {
	http: { idle_timeout: 0.25 },
	# Admit the concurrent children immediately, so that their short lifetimes do not depend on queued scheduling.
	runner: { overcommit: { cpu: 128, memory: 128 } },
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			// Store the commands, so that the spawns send their attaches without waiting for a response.
			let commands = await Promise.all(
				Array.from({ length: 50 }, async (_, index) => {
					let command = await tg.command(child, index);
					await command.store();
					return command;
				}),
			);

			// Wait for a process to attach to, so that no request keeps the connection from becoming idle while JavaScript blocks.
			let spawned = await tg.spawn(child, -1).sandbox();
			await spawned.wait();

			// Queue enough attaches to expose the race with the idle connection closing.
			let attaches = Array.from({ length: 50 }, () => {
				return tg.Process.attach(spawned.id, { tokens: spawned.tokens });
			});
			let spawns = commands.map((command) => {
				return tg.spawn(command).sandbox().then((process) => process);
			});
			let start = Date.now();
			while (Date.now() - start < 1000) {}
			let processes = await Promise.all([...attaches, ...spawns]);
			for (let process of processes) {
				tg.assert((await process.wait()).exit === 0);
			}
			return "done";
		}

		export let child = (index: number) => index;
	'
}

let process = tg build --detach --verbose $path | from json
let outcome = tg wait $process.process | from json
if $outcome.exit != 0 {
	print (tg get --no-tokens --depth inf $outcome.error)
}
assert equal $outcome.exit 0 'the queued attaches should succeed'
assert equal $outcome.output 'done'
let children = tg process children $process.process | from json
assert equal ($children | length) 51 'each process should spawn once'
