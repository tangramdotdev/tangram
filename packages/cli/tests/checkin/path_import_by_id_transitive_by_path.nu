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

# Create inner package that imports by ID with path option, and outer that imports by path.
let path = artifact {
	inner: {
		tangram.ts: $'
			import { helper } from "($dep_id)" with { path: "lib/utils.tg.ts" };
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
	            "contents": tg.blob("import { helper } from \"dir_01sqqe6wh137xnctptzwprcm7pp9bye8nbqw79701ph4b63bhdzsr0\" with { path: \"lib/utils.tg.ts\" };"),
	            "dependencies": {
	              "dir_01sqqe6wh137xnctptzwprcm7pp9bye8nbqw79701ph4b63bhdzsr0?path=lib/utils.tg.ts": {
	                "item": tg.file({
	                  "contents": tg.blob("export const helper = () => \"helper\";"),
	                  "module": "ts",
	                }),
	                "options": {
	                  "id": "dir_01sqqe6wh137xnctptzwprcm7pp9bye8nbqw79701ph4b63bhdzsr0",
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
