use ../../test.nu *

let server = spawn

# Tag the dependencies.
let a1_path = artifact {
	tangram.ts: ''
}
run tg tag a/1.0.0 $a1_path

let b_path = artifact {
	foo.tg.ts: '
		import * as b from "./tangram.ts";
	'
	tangram.ts: '
		import * as a from "a/*";
		import * as foo from "./foo.tg.ts";
	'
}
run tg tag b/1.0.0 $b_path

let a11_path = artifact {
	tangram.ts: '
		import * as b from "b/*";
	'
}
run tg tag a/1.1.0 $a11_path

let path = artifact {
	tangram.ts: '
		import * as a from "a/*";
		import * as b from "b/*";
	'
}

let id = run tg checkin $path
run tg index

let object = run tg object get --blobs --depth=inf --pretty $id
snapshot -n object $object

let metadata = run tg object metadata --pretty $id
snapshot -n metadata $metadata

# This should create a lockfile since it has tagged dependencies.
let lockfile_path = $path | path join 'tangram.lock'
let lock = open $lockfile_path | from json
snapshot -n lock ($lock | to json -i 2)
