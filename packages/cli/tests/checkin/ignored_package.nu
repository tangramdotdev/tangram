use ../../test.nu *

# Checking in a package respects both the root and the nested .tangramignore files when selecting entries.

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

let id = tg checkin ($path | path join 'ignored')
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot --name object $object

let metadata = tg object metadata --pretty $id
snapshot --name metadata $metadata

let lockfile_path = $path | path join 'ignored' 'tangram.lock'
assert (not ($lockfile_path | path exists))
