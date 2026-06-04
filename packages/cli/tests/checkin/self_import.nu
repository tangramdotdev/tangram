use ../../test.nu *

# Checking in a package whose module imports itself produces the expected object and writes no lockfile.

let server = spawn

let path = artifact {
	tangram.ts: '
		import * as self from "./tangram.ts";
	'
}

let id = tg checkin $path
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot --name object $object

let metadata = tg object metadata --pretty $id
snapshot --name metadata $metadata

let lockfile_path = $path | path join 'tangram.lock'
assert (not ($lockfile_path | path exists))
