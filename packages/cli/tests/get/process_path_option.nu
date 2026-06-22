use ../../test.nu *

# A process reference with a path option fails because processes do not contain paths.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.file("contents");',
}
let spawned = tg build --detach --verbose $path | from json

let output = tg get $"($spawned.process)?path=foo" | complete
failure $output
snapshot ($output.stderr | redact $path) '
	error an error occurred
	-> failed to get the reference
	   reference = <process>?path=foo
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to get the reference
	   reference = <process>?path=foo
	-> cannot get path in process

'
