use ../../test.nu *

# Checking in a package that imports by relative path a dependency which itself imports by object ID with a get path option resolves the full transitive graph.

let server = spawn

# Create a directory with nested structure and checkin to get an ID for inner dependency.
let dep_path = artifact {
	lib: {
		utils.tg.ts: 'export function helper() { return "helper"; }'
	}
	tangram.ts: 'export default function () { return "root"; }'
}
let dep_id = tg checkin $dep_path

# Create inner package that imports by ID with path option, and outer that imports by path.
let path = artifact {
	inner: {
		tangram.ts: $'
			import { helper } from "($dep_id)" with { get: "lib/utils.tg.ts" };
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
	        "node": tg.directory({
	          "tangram.ts": tg.file({
	            "contents": tg.blob("import { helper } from \"dir_01hhyz896t5f9pajra9dettatp06zh39zrjhcmffcecjanq5fv8ha0\" with { get: \"lib/utils.tg.ts\" };"),
	            "dependencies": {
	              "dir_01hhyz896t5f9pajra9dettatp06zh39zrjhcmffcecjanq5fv8ha0?get=lib/utils.tg.ts": {
	                "node": tg.file({
	                  "contents": tg.blob("export function helper() { return \"helper\"; }"),
	                  "module": "ts",
	                }),
	                "options": {
	                  "id": "dir_01hhyz896t5f9pajra9dettatp06zh39zrjhcmffcecjanq5fv8ha0",
	                  "path": "lib/utils.tg.ts",
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
