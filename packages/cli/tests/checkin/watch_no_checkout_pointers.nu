use ../../test.nu *

# A watched package checked in with --no-checkout-pointers resolves a tag dependency once it is created and the module contents remain readable.

let server = spawn

let path = artifact {
	tangram.ts: '
		import * as a from "a/^1";
	'
}

let id1 = tg checkin --watch --unsolved-dependencies --no-checkout-pointers $path
tg index

let object1 = tg object get --blobs --depth=inf --pretty $id1
snapshot $object1 '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import * as a from \"a/^1\";"),
	    "dependencies": {
	      "a/^1": null,
	    },
	    "module": "ts",
	  }),
	})
'

let a = artifact {
	tangram.ts: ''
}
tg tag -p a/1.0.0 $a

let id2 = tg checkin --watch --no-checkout-pointers $path
tg index

let object2 = tg object get --blobs --depth=inf --pretty $id2
snapshot $object2 '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import * as a from \"a/^1\";"),
	    "dependencies": {
	      "a/^1": {
	        "node": tg.directory({
	          "tangram.ts": tg.file({
	            "contents": tg.blob(""),
	            "module": "ts",
	          }),
	        }),
	        "options": {
	          "id": "dir_01cgtjawrfbax2hwwc6116mc56tcnft7rp89x88qca7sjs2r6v2wm0",
	          "tag": "a/1.0.0",
	        },
	      },
	    },
	    "module": "ts",
	  }),
	})
'

# Verify we can read the file contents using tg read.
let contents = tg read $"($id2)?get=tangram.ts"
snapshot $contents 'import * as a from "a/^1";'
