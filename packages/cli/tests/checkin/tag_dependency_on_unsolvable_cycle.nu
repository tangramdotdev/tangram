# This test checks in an unsolvable cycle, tags it, then checks in another package that imports it by tag, demonstrating that we can check in dependencies on pre-solved unsolvable cycles.

use ../../test.nu *

let server1 = spawn
let server2 = spawn -c { indexer: false }

# Create and tag a package with a cycle.
let cycle_path = artifact {
	tangram.ts: '
		import "./foo.tg.ts";
	'
	foo.tg.ts: '
		import "./tangram.ts";
	'
}
tg -u $server1.url tag cycle ($cycle_path | path join 'foo.tg.ts')
tg -u $server2.url tag cycle ($cycle_path | path join 'foo.tg.ts')

# Force the tag to be indexed.
tg -u $server1.url index

# Create a package that imports the cycle by tag.
let path = artifact {
	tangram.ts: '
		import * as cycle from "cycle";
	'
}

let id1 = tg -u $server1.url checkin $path --no-lock
let object1 = tg -u $server1.url get --blobs --depth=inf --pretty $id1

let id2 = tg -u $server2.url checkin $path --no-lock
let object2 = tg -u $server2.url object get --blobs --depth=inf --pretty $id2

assert equal $id1 $id2
snapshot -n object $object1
snapshot -n object $object2

tg -u $server1.url index
let metadata = tg -u $server1.url object metadata --pretty $id1
snapshot -n metadata $metadata
