use ../../test.nu *

# Cancelling a finished process with an invalid lease token fails with an already-finished error.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () { return "Hello, World!"; }
	'
}
let process = tg build --detach --verbose $path | from json
tg wait $process.process

let output = tg cancel $process.process invalidtoken | complete
failure $output
snapshot ($output.stderr | redact $path) '
	error an error occurred
	-> failed to cancel the process
	   id = <process>
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to cancel the process
	   id = <process>
	-> failed to cancel the process
	   id = <process>
	-> the process is already finished

'
