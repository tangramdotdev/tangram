use ../../test.nu *

# Cancelling a finished process with an invalid lease succeeds.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default function () { return "Hello, World!"; }
	'
}
let process = tg build --detach --verbose $path | from json
tg wait $process.process

tg cancel $process.process invalidlease
