use ../../test.nu *

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

let id = run tg checkin ($path | path join 'a')
run tg index

let object = run tg object get --blobs --depth=inf --pretty $id
snapshot -n object $object

let metadata = run tg object metadata --pretty $id
snapshot -n metadata $metadata

let lockfile_path = $path | path join 'a' 'tangram.lock'
assert (not ($lockfile_path | path exists))
