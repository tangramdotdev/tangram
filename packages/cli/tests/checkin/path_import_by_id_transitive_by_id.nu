use ../../test.nu *

let server = spawn

# Create a directory with nested structure and checkin to get an ID for inner dependency.
let dep_path = artifact {
	lib: {
		utils.tg.ts: 'export const helper = () => "helper";'
	}
	tangram.ts: 'export default () => "root";'
}
let dep_id = tg checkin $dep_path

# Create inner package that imports by ID with path option.
let inner_path = artifact {
	tangram.ts: $'
		import { helper } from "($dep_id)" with { path: "lib/utils.tg.ts" };
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
	    "contents": tg.blob("import * as inner from \"dir_01m5p1nhzytngtxrhrass4kzejycet3y1hpbhgvcywcy26by9szeng\";"),
	    "dependencies": {
	      "dir_01m5p1nhzytngtxrhrass4kzejycet3y1hpbhgvcywcy26by9szeng": {
	        "item": tg.directory({
	          "tangram.ts": tg.file({
	            "contents": tg.blob("import { helper } from \"dir_01sqqe6wh137xnctptzwprcm7pp9bye8nbqw79701ph4b63bhdzsr0\" with { path: \"lib/utils.tg.ts\" };"),
	            "dependencies": {
	              "dir_01sqqe6wh137xnctptzwprcm7pp9bye8nbqw79701ph4b63bhdzsr0?path=lib/utils.tg.ts": {
	                "item": tg.file({
	                  "contents": tg.blob("export const helper = () => \"helper\";"),
	                  "module": "ts",
	                }),
	                "id": "dir_01sqqe6wh137xnctptzwprcm7pp9bye8nbqw79701ph4b63bhdzsr0",
	                "path": "lib/utils.tg.ts",
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
