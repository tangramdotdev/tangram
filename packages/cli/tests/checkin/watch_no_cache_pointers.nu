use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		import * as a from "a/^1";
	'
}

let id1 = tg checkin --watch --unsolved-dependencies --no-cache-pointers $path
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
tg tag a/1.0.0 $a

let id2 = tg checkin --watch --no-cache-pointers $path
tg index

let object2 = tg object get --blobs --depth=inf --pretty $id2
snapshot $object2 '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import * as a from \"a/^1\";"),
	    "dependencies": {
	      "a/^1": {
	        "item": tg.directory({
	          "tangram.ts": tg.file({
	            "contents": tg.blob(""),
	            "module": "ts",
	          }),
	        }),
	        "options": {
	          "id": "dir_01v4bwqyn57zx4trrnazd8e1begwss9pczeaf67cfwqfezdmzwwt90",
	          "tag": "a/1.0.0",
	        },
	      },
	    },
	    "module": "ts",
	  }),
	})
'

# Verify we can read the file contents using tg read.
let contents = tg read $"($id2)?path=tangram.ts"
snapshot $contents 'import * as a from "a/^1";'
