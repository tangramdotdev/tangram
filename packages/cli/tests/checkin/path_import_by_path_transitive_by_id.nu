use ../../test.nu *

let server = spawn

# Create a directory with nested structure where inner package imports sibling by path with path option.
let inner_root = artifact {
	sibling: {
		lib: {
			utils.tg.ts: 'export const helper = () => "helper";'
		}
		tangram.ts: "import * as utils from ./lib/utils.tg.ts;"
	}
	package: {
		tangram.ts: '
			import { helper } from "../sibling" with { path: "lib/utils.tg.ts" };
		'
	}
}
let inner_id = tg checkin ($inner_root | path join 'package')

# Create outer package that imports inner by ID.
let outer_path = artifact {
	tangram.ts: $'
		import * as inner from "($inner_id)";
	'
}

# Checkin outer package and verify the snapshot.
let id = tg checkin $outer_path
tg index
let object = tg object get --blobs --depth=inf --pretty $id
snapshot $object '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import * as inner from \"dir_019kh7jnxz71er60sktvsaqfyy3n9s87cfxrekhnyaqebb7r1vazt0\";"),
	    "dependencies": {
	      "dir_019kh7jnxz71er60sktvsaqfyy3n9s87cfxrekhnyaqebb7r1vazt0": {
	        "item": tg.directory({
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
	        }),
	      },
	    },
	    "module": "ts",
	  }),
	})
'
