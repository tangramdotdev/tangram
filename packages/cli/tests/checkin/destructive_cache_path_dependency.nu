use ../../test.nu *

# A destructive checkin of a package with a sibling path dependency resolves and embeds the dependency.

let server = spawn

let path = artifact {
	foo: {
		tangram.ts: 'import * as bar from "../bar";'
	}
	bar: {
		tangram.ts: 'export default function () { return "bar"; }'
	}
}
let id = tg checkin --destructive $path --ignore=false
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot $object '
	tg.directory({
	  "bar": tg.directory({
	    "tangram.ts": tg.file({
	      "contents": tg.blob("export default function () { return \"bar\"; }"),
	      "module": "ts",
	    }),
	  }),
	  "foo": tg.directory({
	    "tangram.ts": tg.file({
	      "contents": tg.blob("import * as bar from \"../bar\";"),
	      "dependencies": {
	        "../bar": {
	          "item": tg.directory({
	            "tangram.ts": tg.file({
	              "contents": tg.blob("export default function () { return \"bar\"; }"),
	              "module": "ts",
	            }),
	          }),
	          "options": {
	            "path": "../bar",
	          },
	        },
	      },
	      "module": "ts",
	    }),
	  }),
	})
'
