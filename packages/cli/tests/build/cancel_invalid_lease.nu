use ../../test.nu *

# Cancelling a process with an invalid lease token fails with a missing-lease error, while cancelling with the valid lease succeeds.

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

let process = tg build --detach --verbose $path | from json

let output = tg cancel $process.process invalidtoken | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to cancel the process
	   id = pcs_0000000000000000000000000000
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to cancel the process
	   id = pcs_0000000000000000000000000000
	-> failed to cancel the process
	   id = pcs_0000000000000000000000000000
	-> the process lease was not found

'

tg cancel $process.process $process.lease
tg wait $process.process
