# This test checks in a module inside a package that forms a cycle with another module, demonstrating that root detection works and that a reference artifact is correctly created for the path even if it is not the root.

use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		import "./foo.tg.ts";
	'
	foo.tg.ts: '
		import "./tangram.ts";
	'
}

let id = run tg checkin ($path | path join 'foo.tg.ts')
run tg index

let object = run tg object get --blobs --depth=inf --pretty $id
snapshot -n object $object

let metadata = run tg object metadata --pretty $id
snapshot -n metadata $metadata

let lockfile_path = $path | path join 'tangram.lock'
assert (not ($lockfile_path | path exists))
