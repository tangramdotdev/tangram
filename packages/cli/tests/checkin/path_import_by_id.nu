use ../../test.nu *

# Checking in a package that imports a dependency by object ID with a get path option resolves to the artifact at that path.

let server = server spawn

# Create a directory with nested structure and checkin to get an ID.
let dep_path = artifact {
	lib: {
		utils.tg.ts: 'export function helper() { return "helper"; }'
	}
	tangram.ts: 'export default function () { return "root"; }'
}
let dep_id = tg checkin $dep_path

# Create a package that imports by ID with a path option.
let test_path = artifact {
	tangram.ts: $'
		import { helper } from "($dep_id)" with { get: "lib/utils.tg.ts" };
	'
}

# Checkin and verify the snapshot.
let id = tg checkin $test_path
tg index
let object = tg object get --blobs --depth=inf --pretty $id
snapshot $object '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import { helper } from \"dir_01xgcmweyrtb2fbjzqrt5sgvtjnsf2thm6e8nqd21kyszykspf2nb0\" with { get: \"lib/utils.tg.ts\" };"),
	    "dependencies": {
	      "dir_01xgcmweyrtb2fbjzqrt5sgvtjnsf2thm6e8nqd21kyszykspf2nb0?get=lib/utils.tg.ts": {
	        "node": tg.file({
	          "contents": tg.blob("export function helper() { return \"helper\"; }"),
	          "module": "ts",
	        }),
	        "options": {
	          "id": "dir_01xgcmweyrtb2fbjzqrt5sgvtjnsf2thm6e8nqd21kyszykspf2nb0",
	          "path": "lib/utils.tg.ts",
	        },
	      },
	    },
	    "module": "ts",
	  }),
	})
'
