use ../../test.nu *

# Cancelling a process with an invalid lease token fails with a missing-lease error, while cancelling with the valid lease succeeds.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			while (true) {
				await tg.sleep(1);
			}
		};
	'
}

let process = tg build --detach --verbose $path | from json

let output = tg cancel $process.process invalidtoken | complete
failure $output
assert ($output.stderr | str contains 'the process lease was not found') "the error should mention the missing lease"

tg cancel $process.process $process.lease
tg wait $process.process
