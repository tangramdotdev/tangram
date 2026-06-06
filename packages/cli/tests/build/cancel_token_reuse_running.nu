use ../../test.nu *

# A lease token that was already used to cancel cannot be reused while the process is still running.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			while (true) {
				await tg.sleep(1);
			}
		};
	'
}

# Two detached builds of the same module deduplicate to one process with distinct leases.
let first = tg build --detach --verbose $path | from json
let second = tg build --detach --verbose $path | from json
assert equal $second.process $first.process "the builds should deduplicate to one process"

# Cancel the first lease. The second lease keeps the process running.
tg cancel $first.process $first.lease

# Reusing the first token fails because the lease no longer exists.
let output = tg cancel $first.process $first.lease | complete
failure $output
assert ($output.stderr | str contains 'the process lease was not found') "the error should mention the missing lease"

tg cancel $second.process $second.lease
tg wait $second.process
