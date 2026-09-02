use ../../test.nu *
use ../lib/checkin.nu checkin-output

# A newly created pointer artifact receives the permission computed for its graph subtree.

let server = server spawn
let directory = artifact {
	a.tg.ts: 'import "./b.tg.ts";'
	b.tg.ts: 'import "./a.tg.ts";'
}
let path = $directory | path join a.tg.ts
let output = checkin-output $server $path
assert equal $output.permissions [object_subtree] "the pointer artifact should have subtree permission"
