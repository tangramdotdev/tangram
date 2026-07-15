use ../../test.nu *

# A lease token that was used to cancel a process cannot be used to cancel it again.

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

let output = tg cancel $process.process $process.lease | complete
failure $output
snapshot ($output.stderr | redact) '
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
