use ../../test.nu *

let server = spawn

let path = artifact {
	.tangramignore: '/ignored'
	ignored: {
		.tangramignore: 'foo.txt'
		tangram.ts: 'import * as dependency from "./dependency.tg.ts";'
		dependency.tg.ts: ''
		foo.txt: 'hello, foo'
	}
	tangram.ts: ''
}

let id = run tg checkin ($path | path join 'ignored')
run tg index

let object = run tg object get --blobs --depth=inf --pretty $id
snapshot -n object $object

let metadata = run tg object metadata --pretty $id
snapshot -n metadata $metadata

let lockfile_path = $path | path join 'ignored' 'tangram.lock'
assert (not ($lockfile_path | path exists))
