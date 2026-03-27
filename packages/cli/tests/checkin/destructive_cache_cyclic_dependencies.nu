use ../../test.nu *

let server = spawn

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
