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

let id = run tg checkin $path
run tg index

let object = run tg object get --blobs --depth=inf --pretty $id
snapshot -n object $object

let metadata = run tg object metadata --pretty $id
snapshot -n metadata $metadata

# The old lockfile should be removed since it was out of date.
let lockfile_path = $path | path join 'tangram.lock'
assert (not ($lockfile_path | path exists))
