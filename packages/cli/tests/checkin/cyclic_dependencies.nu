use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	directory: {
		foo: {
			tangram.ts: 'import * as bar from "../bar";'
		}
		bar: {
			tangram.ts: 'import * as foo from "../foo";'
		}
	}
}

let id = tg checkin ($path | path join 'directory' 'foo')
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot -n object $object

let metadata = tg object metadata --pretty $id
snapshot -n metadata $metadata

let lockfile_path = $path | path join 'directory' 'foo' 'tangram.lock'
assert (not ($lockfile_path | path exists))
