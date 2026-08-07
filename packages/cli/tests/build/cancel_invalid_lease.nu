use ../../test.nu *

# Cancelling with an invalid lease is an idempotent no-op, while cancelling with the valid lease stops the process.

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

let output = tg cancel $process.process invalidlease | complete
success $output
assert equal (tg status --timeout 0 $process.process | from json) [started] "the invalid lease should not stop the process"

tg cancel $process.process $process.lease
tg wait $process.process
