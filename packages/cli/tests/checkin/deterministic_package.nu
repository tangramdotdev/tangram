use ../../test.nu *

let server = spawn

# Tag the a dependency.
let a_path = artifact {
	tangram.ts: '// a 1.0.0'
}
tg tag a/1.0.0 $a_path

let a_path = artifact {
	tangram.ts: '// a 1.1.0'
}
tg tag a/1.1.0 $a_path

let id = tg tag get a/1.0.0 | from json | get 'item'
let lock = {
	nodes: [
		{
			kind: "directory",
			entries: {
				"tangram.ts": {
					index: 1,
					kind: "file",
				}
			}
		},
		{
			kind: "file",
			dependencies: {
				"a/^1": {
					item: null,
					options: {
						id: $id,
						tag: "a/1.0.0"
					}
				}
			}
			module: "ts"
		},
	]
} | to json
let path = artifact {
	tangram.ts: '
		import a from "a/^1";
	'
	tangram.lock: $lock

}

let id = tg checkin $path --deterministic
tg index

let object = tg get --depth=inf --pretty $id
snapshot $object '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": blb_01mzk6yctk6vb8f7k35qw07218x9mv26x7kaxq78ynd1ym1an10x8g,
	    "dependencies": {
	      "a/^1": {
	        "item": tg.directory({
	          "tangram.ts": tg.file({
	            "contents": blb_01eywswh4akk7cwdacc6g728n2jj5cj4d3ay6nbxt694326b73qshg,
	            "module": "ts",
	          }),
	        }),
	        "options": {
	          "id": "dir_01s98pk3ygcmxvhmr2f61y37hfyqsktrcrw808rpmhz603pfkm6azg",
	          "tag": "a/1.0.0",
	        },
	      },
	    },
	    "module": "ts",
	  }),
	})
'