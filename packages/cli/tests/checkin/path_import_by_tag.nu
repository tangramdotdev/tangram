use ../../test.nu *

# Checking in a package that imports a tagged dependency with a get path option resolves to the artifact at that path.

let server = spawn

# Create a directory with nested structure and tag it.
let dep_path = artifact {
	lib: {
		utils.tg.ts: 'export function helper() { return "helper"; }'
	}
	tangram.ts: 'export default function () { return "root"; }'
}
tg tag my-lib $dep_path

# Create a package that imports by tag with a path option.
let test_path = artifact {
	tangram.ts: '
		import { helper } from "my-lib" with { get: "lib/utils.tg.ts" };
	'
}

# Checkin and verify the snapshot.
let id = tg checkin $test_path
tg index
let object = tg object get --blobs --depth=inf --pretty $id
snapshot $object '
	tg.directory({
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
	})
'
