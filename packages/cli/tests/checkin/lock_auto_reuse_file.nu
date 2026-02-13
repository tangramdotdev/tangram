use ../../test.nu *

# Test that the default lock mode reuses an existing lockfile for a file dependency lock.

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
	foo.tg.ts: '
		import a from "a/^1";
	'
	foo.tg.lock: $lock
}

tg checkin ($path | path join 'foo.tg.ts') --update a

let lock = open ($path | path join 'foo.tg.lock') | from json
assert (($lock | get nodes.0.dependencies."a/^1".options.tag) == "a/1.1.0")

# The xattr should not exist.
let xattrs = xattr_list ($path | path join 'foo.tg.ts') | where { |name| $name == 'user.tangram.lock' }
assert ($xattrs | is-empty)
