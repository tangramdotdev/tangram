use ../../test.nu *

# Checking in a module that imports its own enclosing package via the current directory produces the expected object and writes no lockfile.

let server = spawn

let path = artifact {
	a: {
		mod.tg.ts: 'import * as a from ".";'
		tangram.ts: ''
	}
}

let id = tg checkin $path
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot --name object $object

let metadata = tg object metadata --pretty $id
snapshot --name metadata $metadata

let lockfile_path = $path | path join 'tangram.lock'
assert (not ($lockfile_path | path exists))
