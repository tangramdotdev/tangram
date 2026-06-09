use ../../test.nu *

# Checking out to a path that already exists fails without the force flag.

let server = spawn

let artifact = artifact {
	tangram.ts: 'export default () => tg.file("Hello, World!");',
}
let id = tg build $artifact | str trim

let dir = mktemp --directory
let path = $dir | path join "out"
tg checkout $id $path

let output = tg checkout $id $path | complete
failure $output
assert ($output.stderr | str contains "there is an existing file system object at the path") "the error should mention the existing file system object"
