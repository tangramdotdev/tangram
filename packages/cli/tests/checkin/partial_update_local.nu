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

let b_path = artifact {
	tangram.ts: '// b 1.0.0'
}
tg tag b/1.0.0 $b_path

let a_id = tg tag get a/1.0.0 | from json | get 'item'
let b_id = tg tag get b/1.0.0 | from json | get 'item'
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
						id: $a_id,
						tag: "a/1.0.0"
					}
				}
			}
			module: "ts"
		},
	]
} | to json
let path = artifact {
	root: {
		tangram.ts: '
			import a from "a/^1" with { local: "../a" }
			import b from "b/^1";
		'
	}
	a: {
		tangram.ts: "// local version of a"
	}
	tangram.lock: $lock

}

let id = tg checkin ($path | path join 'root') --update a
tg index

let object = tg get --depth=inf --pretty $id
snapshot $object '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": blb_01dyj4m1cfvkgerkcxb48kt9cghwm58wq44a9qz81vn4m9mmaxk1ng,
	    "dependencies": {
	      "a/^1?local=../a": {
	        "item": tg.directory({
	          "tangram.ts": tg.file({
	            "contents": blb_01r6mwmke66z61dwjakzxtmgxnkwe8983mrvz4q5pskt4hn4n0xye0,
	            "module": "ts",
	          }),
	        }),
	        "options": {
	          "path": "../a",
	        },
	      },
	      "b/^1": {
	        "item": tg.directory({
	          "tangram.ts": tg.file({
	            "contents": blb_01fvxej6sm4nwrxep5554hdh2wbxf8q4zb7nbw8r0qhw746d5wd3dg,
	            "module": "ts",
	          }),
	        }),
	        "options": {
	          "id": "dir_01q463vyjr6q4c6b83k3ndfwd6dhbfm8c9af7g26b9j5c83x50ccw0",
	          "tag": "b/1.0.0",
	        },
	      },
	    },
	    "module": "ts",
	  }),
	})
'
