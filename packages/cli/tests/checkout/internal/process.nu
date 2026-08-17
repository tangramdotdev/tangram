use ../../../test.nu *

# Checking out a process fails because a process is not an object.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return "hello"; }'
}
let process = tg build --detach $path | str trim
tg wait $process

let output = tg checkout $process | complete
failure $output
snapshot --normalize --redact $path $output.stderr '
	error an error occurred
	-> expected an object ID
	   kind = pcs

'
