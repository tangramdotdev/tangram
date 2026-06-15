use ../../test.nu *

# A process reference with a path option fails because processes do not contain paths.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.file("contents");',
}
let spawned = tg build --detach --verbose $path | from json

let output = tg get $"($spawned.process)?path=foo" | complete
failure $output
assert ($output.stderr | str contains "cannot get path in process") "the error should mention the path option"
