use ../../test.nu *

# Checking in a package that imports by relative path a dependency which itself imports by tag with a get path option resolves the full transitive graph.

let server = spawn

# Create a directory with nested structure and tag it for inner dependency.
let dep_path = artifact {
	lib: {
		utils.tg.ts: 'export function helper() { return "helper"; }'
	}
	tangram.ts: 'export default function () { return "root"; }'
}
tg tag my-lib $dep_path

# Create inner package that imports by tag with path option, and outer that imports by path.
let path = artifact {
	inner: {
		tangram.ts: '
			import { helper } from "my-lib" with { get: "lib/utils.tg.ts" };
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
	            "contents": tg.blob("import { helper } from \"my-lib\" with { get: \"lib/utils.tg.ts\" };"),
	            "dependencies": {
	              "my-lib?get=lib/utils.tg.ts": {
	                "item": tg.file({
	                  "contents": tg.blob("export function helper() { return \"helper\"; }"),
	                  "module": "ts",
	                }),
	                "options": {
	                  "id": "dir_016ac1gtqf1n59p5rm2bqztfvxtk7rwj7cgyfvz76d00g1cjt422t0",
	                  "path": "lib/utils.tg.ts",
	                  "tag": "my-lib",
	                },
	              },
	            },
	            "module": "ts",
	          }),
	        }),
	        "options": {
	          "path": "../inner",
	        },
	      },
	    },
	    "module": "ts",
	  }),
	})
'
