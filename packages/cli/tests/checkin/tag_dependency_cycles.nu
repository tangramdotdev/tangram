use ../../test.nu *

# Checking in a package whose tagged dependencies form a cycle through their versions resolves and writes the expected lockfile.

let server = spawn

# Tag the dependencies.
let a1_path = artifact {
	tangram.ts: ''
}
tg tag a/1.0.0 $a1_path

let b_path = artifact {
	foo.tg.ts: '
		import * as b from "./tangram.ts";
	'
	tangram.ts: '
		import * as a from "a/*";
		import * as foo from "./foo.tg.ts";
	'
}
tg tag b/1.0.0 $b_path

let a11_path = artifact {
	tangram.ts: '
		import * as b from "b/*";
	'
}
tg tag a/1.1.0 $a11_path

let path = artifact {
	tangram.ts: '
		import * as a from "a/*";
		import * as b from "b/*";
	'
}

let id = tg checkin $path
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot --name object $object

let metadata = tg object metadata --pretty $id
snapshot --name metadata $metadata

# This should create a lockfile since it has tagged dependencies.
let lockfile_path = $path | path join 'tangram.lock'
let lock = open $lockfile_path | from json
snapshot --name lock ($lock | to json --indent 2)
