use ../../test.nu *

# Test that the default lock mode reuses an existing lockattr for a file dependency lock.

let server = spawn

let a_path = artifact {
	tangram.ts: '// a 1.0.0'
}
tg tag a/1.0.0 $a_path

let a_path = artifact {
	tangram.ts: '// a 1.1.0'
}
tg tag a/1.1.0 $a_path

let a_id = tg tag get a/1.0.0 | from json | get 'item'
let lock = {
	nodes: [
		{
			kind: "file",
			dependencies: {
				"a/^1": {
					item: null,
					options: {
						id: $a_id,
						tag: "a/1.0.0"
					}
				}
			}
			module: "ts"
		},
	]
} | to json

let path = artifact {
	foo.tg.ts: {
		kind: "file",
		executable: false,
		contents: '
			import a from "a/^1";
		',
		xattrs: {
			"user.tangram.lock": $lock,
		}
	}
}

tg checkin ($path | path join 'foo.tg.ts') --update a

let lockfile_path = $path | path join 'foo.tg.lock'
assert (not ($lockfile_path | path exists))

let lock = xattr_read 'user.tangram.lock' ($path | path join 'foo.tg.ts') | from json
assert (($lock | get nodes.0.dependencies."a/^1".options.tag) == "a/1.1.0")
