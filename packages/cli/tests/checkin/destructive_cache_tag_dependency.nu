use ../../test.nu *

let server = spawn

# Check that using a tag dependency in the cache works.
let a_path = artifact {
	tangram.ts: '
		export default () => "a";
	'
}
tg tag a $a_path
tg index

let path = artifact {
	tangram.ts: '
		import a from "a";
	'
}
let id = tg checkin --destructive $path --ignore=false
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot $object '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import a from \"a\";"),
	    "dependencies": {
	      "a": {
	        "item": tg.directory({
	          "tangram.ts": tg.file({
	            "contents": tg.blob("export default () => \"a\";"),
	            "module": "ts",
	          }),
	        }),
	        "options": {
	          "id": "dir_01397yyk1pe2sv1ddct0f0aq0qtxjbjtw55t9d7vke752ezc8at4p0",
	          "tag": "a",
	        },
	      },
	    },
	    "module": "ts",
	  }),
	})
'
