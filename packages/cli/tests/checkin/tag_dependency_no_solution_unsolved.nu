use ../../test.nu *

let server = spawn

let c1 = artifact {
	tangram.ts: ''
}
tg tag c/1.0.0 $c1

let c2 = artifact {
	tangram.ts: ''
}
tg tag c/2.0.0 $c2

let a = artifact {
	tangram.ts: '
		import * as c from "c/^1"
	'
}
tg tag a/1.0.0 $a

let b = artifact {
	tangram.ts: '
		import * as c from "c/^2"
	'
}
tg tag b/1.0.0 $b

let path = artifact {
	tangram.ts: '
		import * as a from "a/*";
		import * as b from "b/*";
	'
}
let id = tg checkin --unsolved-dependencies $path
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot -n object $object

let metadata = tg object metadata --pretty $id
snapshot -n metadata $metadata

let lockfile_path = $path | path join 'tangram.lock'
let lock = open $lockfile_path | from json
snapshot -n lock ($lock | to json -i 2)
