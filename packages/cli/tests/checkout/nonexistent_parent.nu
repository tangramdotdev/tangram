use ../../test.nu *

# Checking out to a path under a nonexistent parent directory fails.

let server = spawn

let artifact = artifact {
	tangram.ts: 'export default function () { return tg.file("Hello, World!"); }',
}
let id = tg build $artifact | str trim

let dir = mktemp --directory
let path = $dir | path join "nope" "child"

let output = tg checkout $id $path | complete
failure $output
snapshot ($output.stderr | redact $path $dir) '
	error an error occurred
	-> failed to canonicalize the path
	-> No such file or directory (os error 2)

'
