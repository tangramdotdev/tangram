use ../../test.nu *

# Caching an object that is not an artifact fails.

let server = spawn

# Build a process so we can reference its command, which is a non-artifact object.
let path = artifact {
	tangram.ts: 'export default function () { return "hello"; }'
}
let process = tg build --detach $path | str trim
tg wait $process
let command = tg get $process | from json | get command

let output = tg cache $command | complete
failure $output
snapshot ($output.stderr | redact $path | normalize_ids) '
	error an error occurred
	-> expected an artifact

'
