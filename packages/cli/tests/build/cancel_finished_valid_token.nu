use ../../test.nu *

# Cancelling a finished process with its lease token succeeds.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () { return "Hello, World!"; }
	'
}
let process = tg build --detach --verbose $path | from json
tg wait $process.process

tg cancel $process.process $process.lease
