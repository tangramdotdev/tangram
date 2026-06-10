use ../../test.nu *

# Checking in a package with conflicting tag version constraints succeeds under --unsolved-dependencies, leaving the conflict unresolved in the lockfile.

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
snapshot --name object $object

let metadata = tg object metadata --pretty $id
snapshot --name metadata $metadata

let lockfile_path = $path | path join 'tangram.lock'
let lock = open $lockfile_path | from json
snapshot --name lock ($lock | to json --indent 2)
