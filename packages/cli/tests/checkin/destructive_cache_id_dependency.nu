use ../../test.nu *

# A destructive checkin of a package that imports a dependency by object ID resolves the dependency from the cache.

let remote = spawn --name 'remote'
let server = spawn --name 'local' --config {
	remotes: {
		default: {
			url: $remote.url
		}
	}
}

let dep_path = artifact {
	tangram.ts: 'export default function () { return "dep"; }'
}
let dep_id = tg checkin --destructive --ignore=false $dep_path
tg index

let path = artifact {
	tangram.ts: $'
		import dep from "($dep_id)";
	'
}
let id = tg checkin --destructive $path --ignore=false
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot $object '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import dep from \"dir_016ctc43914rtbp4e8jcbezmrrmxw3kckj976ypm7ss0sna1awsykg\";"),
	    "dependencies": {
	      "dir_016ctc43914rtbp4e8jcbezmrrmxw3kckj976ypm7ss0sna1awsykg": {
	        "item": tg.directory({
	          "tangram.ts": tg.file({
	            "contents": tg.blob("export default function () { return \"dep\"; }"),
	            "module": "ts",
	          }),
	        }),
	      },
	    },
	    "module": "ts",
	  }),
	})
'
