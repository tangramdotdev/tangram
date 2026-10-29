use ../../util/test.nu *

# Cancelling with a lease that the process does not hold is an idempotent no-op, while cancelling with the valid lease stops the process.

let local = server spawn

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

let output = tg cancel $process.process 06d0000001r010000000000000 | complete
success $output
assert equal (tg process status --timeout 0 $process.process | from json) [started] "a lease that the process does not hold should not stop the process"

tg cancel $process.process $process.lease
tg wait $process.process
