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

let path = artifact {
	foo: {
		tangram.ts: 'import * as bar from "../bar";'
	}
	bar: {
		tangram.ts: 'export default () => "bar";'
	}
}
let id = tg checkin --destructive $path --ignore=false
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot $object '
	tg.directory({
	  "bar": tg.directory({
	    "tangram.ts": tg.file({
	      "contents": tg.blob("export default () => \"bar\";"),
	      "module": "ts",
	    }),
	  }),
	  "foo": tg.directory({
	    "tangram.ts": tg.file({
	      "contents": tg.blob("import * as bar from \"../bar\";"),
	      "dependencies": {
	        "../bar": {
	          "item": tg.directory({
	            "tangram.ts": tg.file({
	              "contents": tg.blob("export default () => \"bar\";"),
	              "module": "ts",
	            }),
	          }),
	          "options": {
	            "path": "../bar",
	          },
	        },
	      },
	      "module": "ts",
	    }),
	  }),
	})
'

# Check that we cannot destructively checkin artifacts with external paths.
let path = artifact {
	foo: {
		tangram.ts: 'import * as bar from "../bar";'
	}
	bar: {
		tangram.ts: ''
	}
}
let output = tg checkin --destructive ($path | path join 'foo') --ignore=false | complete
failure $output "destructive checkin with external path dependencies should fail"

# Check that using a tag dependency in the cache works.
let a_path = artifact {
	tangram.ts: '
		export default () => "a";
	'
}
tg tag a $a_path
tg index

let path = artifact {
	tangram.ts: '
		import a from "a";
	'
}
let id = tg checkin --destructive $path --ignore=false
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot $object '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import a from \"a\";"),
	    "dependencies": {
	      "a": {
	        "item": tg.directory({
	          "tangram.ts": tg.file({
	            "contents": tg.blob("export default () => \"a\";"),
	            "module": "ts",
	          }),
	        }),
	        "options": {
	          "id": "dir_01397yyk1pe2sv1ddct0f0aq0qtxjbjtw55t9d7vke752ezc8at4p0",
	          "tag": "a",
	        },
	      },
	    },
	    "module": "ts",
	  }),
	})
'

# Check for cyclic dependencies.
let path = artifact {
	foo: {
		tangram.ts: 'import * as bar from "../bar";'
	}
	bar: {
		tangram.ts: 'import * as foo from "../foo";'
	}
}
let id = tg checkin --destructive $path --ignore=false
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot $object '
	tg.directory({
	  "bar": {
	    "graph": tg.graph({
	      "nodes": [
	        {
	          "kind": "file",
	          "contents": tg.blob("import * as bar from \"../bar\";"),
	          "dependencies": {
	            "../bar": {
	              "item": {
	                "index": 2,
	                "kind": "directory",
	              },
	              "options": {
	                "path": "../bar",
	              },
	            },
	          },
	          "module": "ts",
	        },
	        {
	          "kind": "directory",
	          "entries": {
	            "tangram.ts": {
	              "index": 0,
	              "kind": "file",
	            },
	          },
	        },
	        {
	          "kind": "directory",
	          "entries": {
	            "tangram.ts": {
	              "index": 3,
	              "kind": "file",
	            },
	          },
	        },
	        {
	          "kind": "file",
	          "contents": tg.blob("import * as foo from \"../foo\";"),
	          "dependencies": {
	            "../foo": {
	              "item": {
	                "index": 1,
	                "kind": "directory",
	              },
	              "options": {
	                "path": "../foo",
	              },
	            },
	          },
	          "module": "ts",
	        },
	      ],
	    }),
	    "index": 2,
	    "kind": "directory",
	  },
	  "foo": {
	    "graph": tg.graph({
	      "nodes": [
	        {
	          "kind": "file",
	          "contents": tg.blob("import * as bar from \"../bar\";"),
	          "dependencies": {
	            "../bar": {
	              "item": {
	                "index": 2,
	                "kind": "directory",
	              },
	              "options": {
	                "path": "../bar",
	              },
	            },
	          },
	          "module": "ts",
	        },
	        {
	          "kind": "directory",
	          "entries": {
	            "tangram.ts": {
	              "index": 0,
	              "kind": "file",
	            },
	          },
	        },
	        {
	          "kind": "directory",
	          "entries": {
	            "tangram.ts": {
	              "index": 3,
	              "kind": "file",
	            },
	          },
	        },
	        {
	          "kind": "file",
	          "contents": tg.blob("import * as foo from \"../foo\";"),
	          "dependencies": {
	            "../foo": {
	              "item": {
	                "index": 1,
	                "kind": "directory",
	              },
	              "options": {
	                "path": "../foo",
	              },
	            },
	          },
	          "module": "ts",
	        },
	      ],
	    }),
	    "index": 1,
	    "kind": "directory",
	  },
	})
'

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
let output = tg checkin --destructive $path --ignore=false | complete
failure $output "destructive checkin should fail when tag dependency is only on the remote"
