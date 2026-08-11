use ../../test.nu *

# A destructive checkin of a package with a cyclic path dependency produces the expected graph object in the cache.

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
	          "kind": "directory",
	          "entries": {
	            "tangram.ts": {
	              "index": 1,
	              "kind": "file",
	            },
	          },
	        },
	        {
	          "kind": "file",
	          "contents": tg.blob("import * as bar from \"../bar\";"),
	          "dependencies": {
	            "../bar": {
	              "node": {
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
	              "node": {
	                "index": 0,
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
	          "kind": "directory",
	          "entries": {
	            "tangram.ts": {
	              "index": 1,
	              "kind": "file",
	            },
	          },
	        },
	        {
	          "kind": "file",
	          "contents": tg.blob("import * as bar from \"../bar\";"),
	          "dependencies": {
	            "../bar": {
	              "node": {
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
	              "node": {
	                "index": 0,
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
	    "index": 0,
	    "kind": "directory",
	  },
	})
'
