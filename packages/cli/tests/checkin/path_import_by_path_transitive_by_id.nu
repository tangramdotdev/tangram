use ../../test.nu *

# Checking in a package that imports by object ID a dependency which itself imports a sibling by relative path with a get path option resolves the full transitive graph.

let server = spawn

# Create a directory with nested structure where inner package imports sibling by path with path option.
let inner_root = artifact {
	sibling: {
		lib: {
			utils.tg.ts: 'export function helper() { return "helper"; }'
		}
		tangram.ts: "import * as utils from ./lib/utils.tg.ts;"
	}
	package: {
		tangram.ts: '
			import { helper } from "../sibling" with { get: "lib/utils.tg.ts" };
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
	    "contents": tg.blob("import * as inner from \"dir_01y4e3v62knfrjq5dxhfp5gtk7em0jy6mfq55x88nbt3yjtmb099tg\";"),
	    "dependencies": {
	      "dir_01y4e3v62knfrjq5dxhfp5gtk7em0jy6mfq55x88nbt3yjtmb099tg": {
	        "item": tg.directory({
	          "tangram.ts": tg.file({
	            "contents": tg.blob("import { helper } from \"../sibling\" with { get: \"lib/utils.tg.ts\" };"),
	            "dependencies": {
	              "../sibling?get=lib/utils.tg.ts": {
	                "item": tg.file({
	                  "contents": tg.blob("export function helper() { return \"helper\"; }"),
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
