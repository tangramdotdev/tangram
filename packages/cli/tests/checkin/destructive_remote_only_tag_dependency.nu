use ../../test.nu *
let remote = spawn --name 'remote'
let server = spawn --name 'local' --config {
	remotes: [{
		name: 'default',
		url: $remote.url
	}]
}

# Check that destructive checkin fails when a tag dependency is only on the remote and not in the local cache.
let remote_dep_path = artifact {
	tangram.ts: '
		export default () => "remote_only";
	'
}
tg -u $remote.url tag remote_dep $remote_dep_path

let path = artifact {
	tangram.ts: '
		import remote_dep from "remote_dep";
	'
}
let id = tg checkin --destructive $path --ignore=false 
let object = tg get $id --depth=inf --blobs --pretty
snapshot $object '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import remote_dep from \"remote_dep\";"),
	    "dependencies": {
	      "remote_dep": {
	        "item": tg.directory({
	          "tangram.ts": tg.file({
	            "contents": tg.blob("export default () => \"remote_only\";"),
	            "module": "ts",
	          }),
	        }),
	        "options": {
	          "id": "dir_01stfjn9km4b3vq5w8bgqc0xdgkwwn4307p5kysj41z1gsvacytp1g",
	          "tag": "remote_dep",
	        },
	      },
	    },
	    "module": "ts",
	  }),
	})
'
