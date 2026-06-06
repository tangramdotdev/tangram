use ../../test.nu *

# A lease token that was used to cancel a process cannot be used to cancel it again.

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

tg cancel $process.process $process.lease
tg wait $process.process

let output = tg cancel $process.process $process.lease | complete
failure $output
assert ($output.stderr | str contains 'the process is already finished') "the error should mention the finished process"
