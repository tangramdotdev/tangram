use ../../test.nu *

let server = spawn

# Create a directory with nested structure and tag it for inner dependency.
let dep_path = artifact {
	lib: {
		utils.tg.ts: 'export const helper = () => "helper";'
	}
	tangram.ts: 'export default () => "root";'
}
tg tag my-lib $dep_path

# Create inner package that imports by tag with path option and tag it.
let inner_path = artifact {
	tangram.ts: '
		import { helper } from "my-lib" with { path: "lib/utils.tg.ts" };
	'
}
tg tag inner-pkg $inner_path

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
	            "contents": tg.blob("import { helper } from \"my-lib\" with { path: \"lib/utils.tg.ts\" };"),
	            "dependencies": {
	              "my-lib?path=lib/utils.tg.ts": {
	                "item": tg.file({
	                  "contents": tg.blob("export const helper = () => \"helper\";"),
	                  "module": "ts",
	                }),
	                "id": "dir_018n8jag5avr8gszdhe0mdx1745nwqvfwgmdv1j3zhs78q5qby86s0",
	                "path": "lib/utils.tg.ts",
	                "tag": "my-lib",
	              },
	            },
	            "module": "ts",
	          }),
	        }),
	        "id": "dir_01g46x43cnrvxa2gcfj40w414k7m0rtgwdw47mnxsx5yd31ng9b3sg",
	        "tag": "inner-pkg",
	      },
	    },
	    "module": "ts",
	  }),
	})
'
