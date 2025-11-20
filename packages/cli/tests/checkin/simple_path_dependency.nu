use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	foo: {
		tangram.ts: 'import * as bar from "../bar";'
	}
	bar: {
		tangram.ts: ''
	}
}

let id = tg checkin ($path | path join 'foo')
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot -n object $object

let metadata = tg object metadata --pretty $id
snapshot -n metadata $metadata

# This should not create a lockfile since it only has a local path dependency.
let lockfile_path = $path | path join 'foo' 'tangram.lock'
assert (not ($lockfile_path | path exists)) "the lockfile should not exist for local path dependencies"
