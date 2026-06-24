use ../../test.nu *

# A destructive checkin of a package that imports a tagged dependency resolves the dependency from the cache.

let server = spawn

# Check that using a tag dependency in the cache works.
let a_path = artifact {
	tangram.ts: '
		export default function () { return "a"; }
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
	            "contents": tg.blob("export default function () { return \"a\"; }"),
	            "module": "ts",
	          }),
	        }),
	        "options": {
	          "id": "dir_01pcwpzhv23bx2x95epjqdkvqt385etfwqqcz3w1g3va19nn9f9ktg",
	          "tag": "a",
	        },
	      },
	    },
	    "module": "ts",
	  }),
	})
'
