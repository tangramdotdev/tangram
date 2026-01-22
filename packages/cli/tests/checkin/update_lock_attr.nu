use ../../test.nu *

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

tg checkin ($path | path join 'foo.tg.ts')

let id = tg checkin ($path | path join 'foo.tg.ts') --update a --lock=attr
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot $object '
	tg.file({
	  "contents": tg.blob("\n\t\t\timport a from \"a/^1\";\n\t\t"),
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

let lockfile_path = $path | path join 'foo.tg.lock'
assert (not ($lockfile_path | path exists))

let lock = xattr_read 'user.tangram.lock' ($path | path join 'foo.tg.ts') | from json | to json
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