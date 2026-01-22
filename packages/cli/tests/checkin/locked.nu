use ../../test.nu *

let server = spawn

# Tag the a dependency.
let a_path = artifact {
	tangram.ts: '// a 1.0.0'
}
tg tag a/1.0.0 $a_path

# Tag the b dependency.
let b_path = artifact {
	tangram.ts: '// b 1.0.0'
}
tg tag b/1.0.0 $b_path

# Simulate a case where a new import was added that's not present in the lock.
let id = tg tag get a/1.0.0 | from json | get 'item'
let lock = {
	nodes: [
		{
			kind: "directory",
			entries: {
				"tangram.ts": {
					index: 1,
					kind: "file",
				}
			}
		},
		{
			kind: "file",
			dependencies: {
				"a/^1": {
					item: null,
					options: {
						id: $id,
						tag: "a/1.0.0"
					}
				}
			}
			module: "ts"
		},
	]
} | to json

let path = artifact {
	tangram.ts: '
		import a from "a/^1";
        import b from "b/^1";
	'
	tangram.lock: $lock
}

# Ensure we cannot checkin this artifact with --locked
let output = tg checkin $path --locked | complete
failure $output

# Ensure we cannot this artifact with --locked and --unsolved-dependencies
let output = tg checkin $path --locked --unsolved-dependencies | complete
failure $output
