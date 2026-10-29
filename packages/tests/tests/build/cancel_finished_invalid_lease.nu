use ../../lib/test.nu *

# Cancelling a finished process with a lease that the process does not hold succeeds.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export default function () { return "Hello, World!"; }
	'
}
let process = tg build --detach --verbose $path | from json
tg wait $process.process

tg cancel $process.process 06d0000001r010000000000000
