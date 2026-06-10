use ../../test.nu *

# Checking in a package whose existing lockfile is out of date discards the stale lockfile and removes it.

let server = spawn

let path = artifact {
	tangram.ts: 'import "./b.tg.ts";'
	b.tg.ts: ''
	tangram.lock: '{
		"nodes": [
			{
				"kind": "directory",
				"entries": {
					"a.tg.ts": { "index": 1, "kind": "file" },
					"tangram.ts": { "index": 2, "kind": "file" }
				}
			},
			{
				"kind": "file"
			},
			{
				"kind": "file",
				"dependencies": {
					"./a.tg.ts": {
						"item": { "index": 0, "kind": "directory" },
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
snapshot --name object $object

let metadata = tg object metadata --pretty $id
snapshot --name metadata $metadata

# The old lockfile should be removed since it was out of date.
let lockfile_path = $path | path join 'tangram.lock'
assert (not ($lockfile_path | path exists))
