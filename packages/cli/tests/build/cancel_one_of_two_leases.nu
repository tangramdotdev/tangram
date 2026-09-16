use ../../test.nu *

# Cancelling one of two leases on a deduplicated process leaves it running, and cancelling the last lease cancels it.

let server = server spawn --config { advanced: { checkpoints: true } }

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
let watch = tg checkpoint watch process.control.response.publish --params '{"kind":"acquire_lease"}' | from json | get watch
let second = timeout 10s tg build --detach --verbose $path | from json
let response = timeout 1s tg checkpoint wait process.control.response.publish $watch 0 | complete
tg checkpoint unwatch process.control.response.publish $watch
assert equal $response.exit_code 124 "the runner should acquire the cached process lease directly"
assert equal $second.process $first.process "the builds should deduplicate to one process"
assert ($second.lease != $first.lease) "each build should hold its own lease"

# Cancelling the first lease leaves the process running.
tg cancel $first.process $first.lease
assert equal (tg process status --timeout 0 $first.process | from json) ["started"] "the process should still be running"

# Cancelling the last lease cancels the process.
tg cancel $second.process $second.lease
let output = tg output $second.process | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to get the process output
	   id = pcs_0000000000000000000000000000
	-> failed to run the process
	   process = pcs_0000000000000000000000000000
	-> the process was canceled

'
