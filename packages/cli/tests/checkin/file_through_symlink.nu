use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	a: {
		tangram.ts: 'import "../b/c/d";'
	}
	b: {
		c: (symlink 'e')
		e: {
			d: 'hello, world!'
		}
	}
}

let id = tg checkin ($path | path join 'a')
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot -n object $object

let metadata = tg object metadata --pretty $id
snapshot -n metadata $metadata

let lockfile_path = $path | path join 'a' 'tangram.lock'
assert (not ($lockfile_path | path exists))
