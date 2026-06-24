use ../../test.nu *

# Checking out to a path that already exists succeeds with the force flag.

let server = spawn

let artifact = artifact {
	tangram.ts: 'export default function () { return tg.file("Hello, World!"); }',
}
let id = tg build $artifact | str trim

let dir = mktemp --directory
let path = $dir | path join "out"
tg checkout $id $path

let output = tg checkout --force $id $path | complete
success $output
assert ((open $path | str trim) == "Hello, World!") "the checked out file should contain the artifact contents"
