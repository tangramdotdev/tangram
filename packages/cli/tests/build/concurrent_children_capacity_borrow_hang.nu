use ../../test.nu *

# With one CPU, a parent holding the runner's only allocation spawns more children
# than there is capacity for, so children run by borrowing the parent's allocation.
# Borrowable capacity is only advertised at spawn time and collapsed into one
# scheduler entry per parent. Once a borrowing child finishes, nothing re-advertises
# the freed capacity, so the parent's remaining children are never scheduled and it
# hangs forever.

let server = spawn --config { runner: { cpus: 1 } }

let path = artifact {
	tangram.ts: '
		export function inner(...args: Array<{ id: string }>) {
			return args[0]?.id ?? "0";
		}
		export default async function () {
			return await Promise.all(
				Array.from({ length: 8 }, (_, i) => tg.build(inner, { id: String(i) })),
			);
		}
	'
}

# The parent must finish; with the bug it hangs forever awaiting children that are
# never scheduled, so tg wait blocks until the harness times the test out.
let top = tg build --detach $path | str trim
let result = tg wait $top | from json
assert ($result.exit == 0) "the build must succeed"
assert equal $result.output ["0", "1", "2", "3", "4", "5", "6", "7"]
