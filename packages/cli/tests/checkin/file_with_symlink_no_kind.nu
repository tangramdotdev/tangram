use ../../test.nu *

# Checking in a package that imports a symlink without an explicit kind produces the expected object and writes no lockfile.

let server = spawn

let path = artifact {
	tangram.ts: '
		import foo from "./foo.tg.ts";
	'
	foo.tg.ts: (symlink 'bar.tg.ts')
	bar.tg.ts: '
		export default bar = "bar";
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
