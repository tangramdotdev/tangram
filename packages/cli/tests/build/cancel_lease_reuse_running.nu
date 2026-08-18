use ../../test.nu *

# Releasing the same lease more than once is an idempotent no-op while the process is still running.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			while (true) {
				await tg.sleep(1);
			}
		}
	'
}

# Two detached builds of the same module deduplicate to one process with distinct leases.
let first = tg build --detach --verbose $path | from json
let second = tg build --detach --verbose $path | from json
assert equal $second.process $first.process "the builds should deduplicate to one process"

# Cancel the first lease. The second lease keeps the process running.
tg cancel $first.process $first.lease

# Reuse the first lease and verify that the second lease keeps the process running.
let output = tg cancel $first.process $first.lease | complete
success $output
assert equal (tg process status --timeout 0 $first.process | from json) [started] "reusing the first lease should not stop the process"

tg cancel $second.process $second.lease
tg wait $second.process
