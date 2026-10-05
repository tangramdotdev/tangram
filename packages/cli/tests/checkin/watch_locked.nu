use ../lib/test.nu *

# --watch with --locked prevents updating a stale lock.

let local = server spawn

let a_path = artifact {
	tangram.ts: '// a 1.0.0'
}
tg tag -p a/1.0.0 $a_path

let b_path = artifact {
	tangram.ts: '// b 1.0.0'
}
tg tag -p b/1.0.0 $b_path

let id = tg tag get a/1.0.0 | from json | get target.id
let lock = {
	nodes: [
		{
			kind: "directory",
			entries: {
				"tangram.ts": 1
			}
		},
		{
			kind: "file",
			dependencies: {
				"a/^1": {
					node: null,
					options: {
						id: $id,
						tag: "a/1.0.0"
					}
				}
			}
			module: "typescript"
		},
	]
} | to json

let path = artifact {
	tangram.ts: '
		import a from "a/^1";
	'
	tangram.lock: $lock
}

# Ensure we can checkin with file watching.
let output = tg checkin --no-tokens $path --watch --locked | complete
success $output

'import b from "b/^1"' | save ($path | path join 'tangram.ts')  --append

# Wait for changes to be handled.
tg watch touch $path ($path | path join 'tangram.ts')

# Checkin again but make sure that it fails.
let output = tg checkin --no-tokens $path --watch --locked | complete
failure $output
