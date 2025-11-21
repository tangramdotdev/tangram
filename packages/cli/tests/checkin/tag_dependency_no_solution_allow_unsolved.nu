use ../../test.nu *

let server = spawn

# Tag the dependencies.
let c1_path = artifact {
	tangram.ts: ''
}
tg tag c/1.0.0 $c1_path

let c2_path = artifact {
	tangram.ts: ''
}
tg tag c/2.0.0 $c2_path

let a_path = artifact {
	tangram.ts: '
		import * as c from "c/^1"
	'
}
tg tag a/1.0.0 $a_path

let b_path = artifact {
	tangram.ts: '
		import * as c from "c/^2"
	'
}
tg tag b/1.0.0 $b_path

let path = artifact {
	tangram.ts: '
		import * as a from "a/*";
		import * as b from "b/*";
	'
}

let id = tg checkin --allow-unsolved-dependencies=true $path
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot -n object $object

let metadata = tg object metadata --pretty $id
snapshot -n metadata $metadata

# This should create a lockfile since it has tagged dependencies.
let lockfile_path = $path | path join 'tangram.lock'
let lock = open $lockfile_path | from json
snapshot -n lock ($lock | to json -i 2)
