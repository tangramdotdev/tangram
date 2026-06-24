use ../../test.nu *

# Checking out to a path that already exists fails without the force flag.

let server = spawn

let artifact = artifact {
	tangram.ts: 'export default function () { return tg.file("Hello, World!"); }',
}
let id = tg build $artifact | str trim

let dir = mktemp --directory
let path = $dir | path join "out"
tg checkout $id $path

let output = tg checkout $id $path | complete
failure $output
snapshot ($output.stderr | redact $path $dir) '
	error an error occurred
	-> failed to check out the artifact
	   artifact = fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60
	-> there is an existing file system object at the path

'
