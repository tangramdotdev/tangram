use ../../test.nu *

let server = spawn

# Create a directory with nested structure.
let path = artifact {
	sibling: {
		lib: {
			utils.tg.ts: 'export const helper = () => "helper";'
		},
		tangram.ts: "import * as utils from ./lib/utils.tg.ts;"
	}
	package: {
		tangram.ts: '
			import { helper } from "../sibling" with { path: "lib/utils.tg.ts" };
		'
	}
}

# Checkin and verify the snapshot.
let id = tg checkin ($path | path join 'package')
tg index
let object = tg object get --blobs --depth=inf --pretty $id
snapshot $object '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import { helper } from \"../sibling\" with { path: \"lib/utils.tg.ts\" };"),
	    "dependencies": {
	      "../sibling?path=lib/utils.tg.ts": {
	        "item": tg.file({
	          "contents": tg.blob("export const helper = () => \"helper\";"),
	          "module": "ts",
	        }),
	        "options": {
	          "path": "../sibling/lib/utils.tg.ts",
	        },
	      },
	    },
	    "module": "ts",
	  }),
	})
'
