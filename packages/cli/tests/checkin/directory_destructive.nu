use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	directory: {
		a: {
			b: {
				c: (symlink '../../a/d/e')
			}
			d: {
				e: (symlink '../../a/f/g')
			}
			f: {
				g: ''
			}
		}
	}
}

let id = run tg checkin --destructive --ignore=false ($path | path join 'directory')
run tg index

let object = run tg object get --blobs --depth=inf --pretty $id
snapshot -n object $object

let metadata = run tg object metadata --pretty $id
snapshot -n metadata $metadata

let lockfile_path = $path | path join 'directory' 'tangram.lock'
assert (not ($lockfile_path | path exists))
