use ../../test.nu *

# Signalling a cacheable process fails because cacheable processes cannot receive signals.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => 42;',
}
let process = tg build --detach $path | str trim
tg wait $process

let output = tg signal $process | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to signal the process
	   id = <process>
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to post process signal
	-> failed to signal the process
	   id = <process>
	-> cannot signal cacheable processes
	   id = <process>

'
