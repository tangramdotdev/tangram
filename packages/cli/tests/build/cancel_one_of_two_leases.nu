use ../../test.nu *

# Cancelling one of two leases on a deduplicated process leaves it running, and cancelling the last lease cancels it.

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
assert ($second.lease != $first.lease) "each build should hold its own lease"

# Cancelling the first lease leaves the process running.
tg cancel $first.process $first.lease
assert equal (tg status --timeout 0 $first.process | from json) ["started"] "the process should still be running"

# Cancelling the last lease cancels the process.
tg cancel $second.process $second.lease
let output = tg output $second.process | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to get the process output
	   id = <process>
	-> the process was canceled

'
