use ../../test.nu *

# A process reference can carry a provenance path, but a get option cannot traverse into a process.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.file("contents"); }',
}
let spawned = tg build --detach --verbose $path | from json

let output = tg --no-quiet get $"($spawned.process)?path=foo" | complete
success $output
assert ($output.stderr | str contains "?path=foo") "the resolved referent should retain the path option"

let output = tg get $"($spawned.process)?get=foo" | complete
failure $output
snapshot --normalize --redact $path $output.stderr '
	error an error occurred
	-> failed to get the reference
	   reference = pcs_0000000000000000000000000000?get=foo
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to get the reference
	   reference = pcs_0000000000000000000000000000?get=foo
	-> cannot apply a get option to a process

'
