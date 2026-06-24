use ../../test.nu *

# Checking in a package that imports by object ID a dependency which itself imports by object ID with a get path option resolves the full transitive graph.

let server = spawn

# Create a directory with nested structure and checkin to get an ID for inner dependency.
let dep_path = artifact {
	lib: {
		utils.tg.ts: 'export function helper() { return "helper"; }'
	}
	tangram.ts: 'export default function () { return "root"; }'
}
let dep_id = tg checkin $dep_path

# Create inner package that imports by ID with path option.
let inner_path = artifact {
	tangram.ts: $'
		import { helper } from "($dep_id)" with { get: "lib/utils.tg.ts" };
	'
}
let inner_id = tg checkin $inner_path

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
	    "contents": tg.blob("import * as inner from \"dir_01rcevvr4cmvv2a5vnn03zzmm9d78d6tcy7mqpydg72k19pcfad1w0\";"),
	    "dependencies": {
	      "dir_01rcevvr4cmvv2a5vnn03zzmm9d78d6tcy7mqpydg72k19pcfad1w0": {
	        "item": tg.directory({
	          "tangram.ts": tg.file({
	            "contents": tg.blob("import { helper } from \"dir_016ac1gtqf1n59p5rm2bqztfvxtk7rwj7cgyfvz76d00g1cjt422t0\" with { get: \"lib/utils.tg.ts\" };"),
	            "dependencies": {
	              "dir_016ac1gtqf1n59p5rm2bqztfvxtk7rwj7cgyfvz76d00g1cjt422t0?get=lib/utils.tg.ts": {
	                "item": tg.file({
	                  "contents": tg.blob("export function helper() { return \"helper\"; }"),
	                  "module": "ts",
	                }),
	                "options": {
	                  "id": "dir_016ac1gtqf1n59p5rm2bqztfvxtk7rwj7cgyfvz76d00g1cjt422t0",
	                  "path": "lib/utils.tg.ts",
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
