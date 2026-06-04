use ../../test.nu *

# A watched package whose tag dependency does not yet exist checks in unsolved, then resolves the dependency once the tag is created.

let server = spawn

# Create a package that depends on a/^1 which does not exist yet.
let path = artifact {
	tangram.ts: '
		import * as a from "a/^1";
	'
}

# Check in with --unsolved-dependencies. The dependency should be unsolved.
let id1 = tg checkin --watch --unsolved-dependencies $path
tg index

let object1 = tg object get --blobs --depth=inf --pretty $id1
snapshot --name object_before $object1

# Now create the tag.
let a = artifact {
	tangram.ts: ''
}
tg tag a/1.0.0 $a

# Check in again without --unsolved-dependencies. The dependency should now be resolved.
let id2 = tg checkin --watch $path
tg index

let object2 = tg object get --blobs --depth=inf --pretty $id2
snapshot --name object_after $object2

let lockfile_path = $path | path join 'tangram.lock'
let lock = open $lockfile_path | from json
snapshot --name lock ($lock | to json --indent 2)
