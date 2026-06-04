use ../../test.nu *

# A destructive checkin of a directory containing chained relative symlinks produces the expected object and writes no lockfile.

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

let id = tg checkin --destructive --ignore=false ($path | path join 'directory')
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot --name object $object

let metadata = tg object metadata --pretty $id
snapshot --name metadata $metadata

let lockfile_path = $path | path join 'directory' 'tangram.lock'
assert (not ($lockfile_path | path exists))
