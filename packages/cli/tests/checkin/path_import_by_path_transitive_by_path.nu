use ../../test.nu *

let server = spawn

# Create a directory with nested structure where inner package imports sibling by path with path option.
let path = artifact {
	sibling: {
		lib: {
			utils.tg.ts: 'export const helper = () => "helper";'
		}
		tangram.ts: "import * as utils from ./lib/utils.tg.ts;"
	}
	inner: {
		tangram.ts: '
			import { helper } from "../sibling" with { path: "lib/utils.tg.ts" };
		'
	}
	outer: {
		tangram.ts: '
			import * as inner from "../inner";
		'
	}
}

# Checkin outer package and verify the snapshot.
let id = tg checkin ($path | path join 'outer')
tg index
let object = tg object get --blobs --depth=inf --pretty $id
snapshot $object '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import * as inner from \"../inner\";"),
	    "dependencies": {
	      "../inner": {
	        "item": tg.directory({
	          "tangram.ts": tg.file({
	            "contents": tg.blob("import { helper } from \"../sibling\" with { path: \"lib/utils.tg.ts\" };"),
	            "dependencies": {
	              "../sibling?path=lib/utils.tg.ts": {
	                "item": tg.file({
	                  "contents": tg.blob("export const helper = () => \"helper\";"),
	                  "module": "ts",
	                }),
	                "path": "../sibling/lib/utils.tg.ts",
	              },
	            },
	            "module": "ts",
	          }),
	        }),
	        "path": "../inner",
	      },
	    },
	    "module": "ts",
	  }),
	})
'
