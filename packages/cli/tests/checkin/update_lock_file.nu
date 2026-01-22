use ../../test.nu *

# Test that --update with --lock=file writes a sibling lockfile.

let server = spawn

# Tag the a dependency versions.
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

let id = tg checkin ($path | path join 'foo.tg.ts') --update a --lock=file
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot $object '
	tg.file({
	  "contents": tg.blob("import a from \"a/^1\";"),
	  "dependencies": {
	    "a/^1": {
	      "item": tg.directory({
	        "tangram.ts": tg.file({
	          "contents": tg.blob("// a 1.1.0"),
	          "module": "ts",
	        }),
	      }),
	      "options": {
	        "id": "dir_01baya75taqzrf1y70pcwgqyzznzsfqeqg7d2bgpqdaj0j8xzxfvq0",
	        "tag": "a/1.1.0",
	      },
	    },
	  },
	  "module": "ts",
	})
'

let lock = open ($path | path join 'foo.tg.lock')
snapshot $lock '
	{
	  "nodes": [
	    {
	      "kind": "file",
	      "dependencies": {
	        "a/^1": {
	          "item": null,
	          "options": {
	            "id": "dir_01baya75taqzrf1y70pcwgqyzznzsfqeqg7d2bgpqdaj0j8xzxfvq0",
	            "tag": "a/1.1.0"
	          }
	        }
	      },
	      "module": "ts"
	    }
	  ]
	}
'

# The xattr should not exist.
let xattrs = xattr_list ($path | path join 'foo.tg.ts') | where { |name| $name == 'user.tangram.lock' }
assert ($xattrs | is-empty)
