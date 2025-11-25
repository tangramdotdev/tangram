use ../../test.nu *

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

let id = run tg checkin $path
run tg index

let object = run tg object get --blobs --depth=inf --pretty $id
snapshot -n object $object

let metadata = run tg object metadata --pretty $id
snapshot -n metadata $metadata

let lockfile_path = $path | path join 'tangram.lock'
assert (not ($lockfile_path | path exists))
