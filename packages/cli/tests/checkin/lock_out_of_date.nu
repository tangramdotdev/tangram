use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: 'import "./b.tg.ts";'
	b.tg.ts: ''
	tangram.lock: '{
		"nodes": [
			{
				"kind": "directory",
				"entries": {
					"a.tg.ts": { "node": 1 },
					"tangram.ts": { "node": 2 }
				}
			},
			{
				"kind": "file"
			},
			{
				"kind": "file",
				"dependencies": {
					"./a.tg.ts": {
						"item": { "node": 0 },
						"path": "./a.tg.ts"
					}
				}
			}
		]
	}'
}

let id = tg checkin $path
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot -n object $object

let metadata = tg object metadata --pretty $id
snapshot -n metadata $metadata

# The old lockfile should be removed since it was out of date.
let lockfile_path = $path | path join 'tangram.lock'
assert (not ($lockfile_path | path exists))
