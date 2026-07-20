use ../../test.nu *

# Cancelling with the same lease token after the process finishes succeeds.

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

tg cancel $process.process $process.lease
tg wait $process.process

tg cancel $process.process $process.lease
