use ../../test.nu *

# Cancelling a finished process with its lease token fails with an already-finished error.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => "Hello, World!";
	'
}
let process = tg build --detach --verbose $path | from json
tg wait $process.process

let output = tg cancel $process.process $process.lease | complete
failure $output
assert ($output.stderr | str contains 'the process is already finished') "the error should mention the finished process"
