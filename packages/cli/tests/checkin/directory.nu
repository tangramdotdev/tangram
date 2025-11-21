use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	hello.txt: 'Hello, world!'
	link: (symlink 'hello.txt')
	subdirectory: {
		sublink: (symlink '../link')
	}
}

let id = run tg checkin $path
run tg index

let object = run tg object get --blobs --depth=inf --pretty $id
snapshot -n object $object

let metadata = run tg object metadata --pretty $id
snapshot -n metadata $metadata

let lockfile_path = $path | path join 'tangram.lock'
assert (not ($lockfile_path | path exists))
