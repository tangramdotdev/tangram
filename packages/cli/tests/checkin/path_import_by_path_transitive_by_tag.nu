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
tg tag inner-pkg ($inner_root | path join 'package')

# Create outer package that imports inner by tag.
let outer_path = artifact {
	tangram.ts: '
		import * as inner from "inner-pkg";
	'
}

# Checkin outer package and verify the snapshot.
let id = tg checkin $outer_path
tg index
let object = tg object get --blobs --depth=inf --pretty $id
snapshot $object '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import * as inner from \"inner-pkg\";"),
	    "dependencies": {
	      "inner-pkg": {
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
	        "id": "dir_019kh7jnxz71er60sktvsaqfyy3n9s87cfxrekhnyaqebb7r1vazt0",
	        "tag": "inner-pkg",
	      },
	    },
	    "module": "ts",
	  }),
	})
'
