use ../../test.nu *
let remote = spawn --name 'remote'
let server = spawn --name 'local' --config {
	remotes: [{
		name: 'default',
		url: $remote.url
	}]
}

let dep_path = artifact {
	tangram.ts: 'export default () => "dep";'
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
	    "contents": tg.blob("import dep from \"dir_01eg1hz5n9tvjmzbsvxgwegxa5jnx10pgpx47k99b9z200v7kqdm7g\";"),
	    "dependencies": {
	      "dir_01eg1hz5n9tvjmzbsvxgwegxa5jnx10pgpx47k99b9z200v7kqdm7g": {
	        "item": tg.directory({
	          "tangram.ts": tg.file({
	            "contents": tg.blob("export default () => \"dep\";"),
	            "module": "ts",
	          }),
	        }),
	      },
	    },
	    "module": "ts",
	  }),
	})
'
