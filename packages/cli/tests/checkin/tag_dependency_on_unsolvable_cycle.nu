# This test checks in an unsolvable cycle, tags it, then checks in another package that imports it by tag, demonstrating that we can check in dependencies on pre-solved unsolvable cycles.

use ../../test.nu *

let server = spawn

# Create and check in a cycle.
let cycle_path = artifact {
	tangram.ts: '
		import "./foo.tg.ts";
	'
	foo.tg.ts: '
		import "./tangram.ts";
	'
}

let cycle_id = tg checkin ($cycle_path | path join 'foo.tg.ts')
tg tag cycle $cycle_id
tg index

# Create a package that imports the cycle by tag.
let path = artifact {
	tangram.ts: '
		import * as cycle from "cycle";
	'
}

let id = tg checkin $path
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot -n object $object

let metadata = tg object metadata --pretty $id
snapshot -n metadata $metadata

# This should create a lockfile since it has a tagged dependency.
let lockfile_path = $path | path join 'tangram.lock'
let lock = open $lockfile_path | from json
snapshot -n lock ($lock | to json -i 2)
