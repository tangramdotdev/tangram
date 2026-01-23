use ../../test.nu *

# Test that --watch with --update selectively updates dependencies.

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

let b_path = artifact {
	tangram.ts: '// b 1.0.0'
}
tg tag b/1.0.0 $b_path

let a_id = tg tag get a/1.0.0 | from json | get 'item'
let b_id = tg tag get b/1.0.0 | from json | get 'item'
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
						id: $a_id,
						tag: "a/1.0.0"
					}
				}
				"b/^1": {
					item: null,
					options: {
						id: $b_id,
						tag: "b/1.0.0"
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

# Check in with --watch.
let output = tg checkin $path --watch --locked | complete
success $output

# Wait for changes to be handled.
tg watch touch $path

let id = tg checkin $path --watch --update 'a'
tg index

let object = tg get --blobs --depth=inf --pretty $id
snapshot $object '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import a from \"a/^1\";\nimport b from \"b/^1\";"),
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
	      "b/^1": {
	        "item": tg.directory({
	          "tangram.ts": tg.file({
	            "contents": tg.blob("// b 1.0.0"),
	            "module": "ts",
	          }),
	        }),
	        "options": {
	          "id": "dir_01q463vyjr6q4c6b83k3ndfwd6dhbfm8c9af7g26b9j5c83x50ccw0",
	          "tag": "b/1.0.0",
	        },
	      },
	    },
	    "module": "ts",
	  }),
	})
'
let lock = open ($path | path join 'tangram.lock')
snapshot $lock '
	{
	  "nodes": [
	    {
	      "kind": "directory",
	      "entries": {
	        "tangram.ts": {
	          "index": 1,
	          "kind": "file"
	        }
	      }
	    },
	    {
	      "kind": "file",
	      "dependencies": {
	        "a/^1": {
	          "item": null,
	          "options": {
	            "id": "dir_01baya75taqzrf1y70pcwgqyzznzsfqeqg7d2bgpqdaj0j8xzxfvq0",
	            "tag": "a/1.1.0"
	          }
	        },
	        "b/^1": {
	          "item": null,
	          "options": {
	            "id": "dir_01q463vyjr6q4c6b83k3ndfwd6dhbfm8c9af7g26b9j5c83x50ccw0",
	            "tag": "b/1.0.0"
	          }
	        }
	      },
	      "module": "ts"
	    }
	  ]
	}
'
