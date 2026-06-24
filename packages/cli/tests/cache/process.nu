use ../../test.nu *

# Caching a process fails because a process is not an object.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return "hello"; }'
}
let process = tg build --detach $path | str trim
tg wait $process

let output = tg cache $process | complete
failure $output
snapshot ($output.stderr | redact $path) '
	error an error occurred
	-> expected an object ID
	   kind = pcs

'
