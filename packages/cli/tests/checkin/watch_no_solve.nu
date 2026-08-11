use ../../test.nu *

# --watch with --no-solve skips dependency resolution.

let server = spawn

# Tag the a dependency.
let a_path = artifact {
	tangram.ts: '// a 1.0.0'
}
tg tag -p a/1.0.0 $a_path

let id = tg tag get a/1.0.0 | from json | get target.id
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
					node: null,
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
	'
	tangram.lock: $lock
}

# Ensure we can checkin with file watching.
let id = tg checkin $path --watch --locked
let object = tg get --blobs --depth=inf --pretty $id
snapshot $object '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import a from \"a/^1\";"),
	    "dependencies": {
	      "a/^1": {
	        "node": tg.directory({
	          "tangram.ts": tg.file({
	            "contents": tg.blob("// a 1.0.0"),
	            "module": "ts",
	          }),
	        }),
	        "options": {
	          "id": "dir_01bf6zscaw3mrpb34z4ykc55pahngr04ma7bvh0g61cjgpjvyfjdf0",
	          "tag": "a/1.0.0",
	        },
	      },
	    },
	    "module": "ts",
	  }),
	})
'

# Update the file and check in with --no-solve.
'// watermark' | save ($path | path join 'tangram.ts') --append

# Wait for changes to be handled.
tg watch touch $path ($path | path join 'tangram.ts')

let id = tg checkin $path --watch --no-solve

let object = tg get --blobs --depth=inf --pretty $id
snapshot $object '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import a from \"a/^1\";// watermark"),
	    "dependencies": {
	      "a/^1": null,
	    },
	    "module": "ts",
	  }),
	})
'
