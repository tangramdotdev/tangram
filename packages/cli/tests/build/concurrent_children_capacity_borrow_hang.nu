use ../../test.nu *

# With one CPU, a parent holding the runner's only allocation spawns more children
# than there is capacity for, so children run by borrowing the parent's allocation.
# Borrowable capacity is only advertised at spawn time and collapsed into one
# scheduler entry per parent. Once a borrowing child finishes, nothing re-advertises
# the freed capacity, so the parent's remaining children are never scheduled and it
# hangs forever.

let server = server spawn --config {
	advanced: {
		checkpoints: true,
	},
	runner: {
		cpus: 1,
	},
}

let path = artifact {
	tangram.ts: '
		export function inner(...args: Array<{ id: string }>) {
			return args[0]?.id ?? "0";
		}
		export default async function () {
			return await Promise.all(
				Array.from({ length: 2 }, (_, i) => tg.build(inner, { id: String(i) })),
			);
		}
	'
}

let finish_watch = (
	tg checkpoint watch runner.process.finish
	| from json
	| get watch
)

# Hold the first borrowing child as it finishes. Once it advances, its capacity
# must become available to the second child.
let top = tg build --detach $path | str trim
tg checkpoint wait runner.process.finish $finish_watch 0 | ignore
tg checkpoint continue runner.process.finish $finish_watch 0
tg checkpoint unwatch runner.process.finish $finish_watch

let result = tg wait $top | from json
assert ($result.exit == 0) "the build must succeed"
assert equal $result.output ["0", "1"]
