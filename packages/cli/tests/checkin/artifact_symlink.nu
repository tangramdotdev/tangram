use ../../test.nu *

# Checking in a package whose import path traverses a symlinked directory resolves to the target artifact and writes no lockfile.

let server = spawn

let path = artifact {
	a: {
		tangram.ts: 'import "../b/c";'
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
snapshot --name object $object

let metadata = tg object metadata --pretty $id
snapshot --name metadata $metadata

let lockfile_path = $path | path join 'a' 'tangram.lock'
assert (not ($lockfile_path | path exists))
