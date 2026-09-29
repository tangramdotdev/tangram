use ../lib/test.nu *

# A destructive checkin of a package with a cyclic path dependency produces the expected graph object.

let local = server spawn

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

let object = tg object get --blobs --depth=inf --no-tokens --pretty $id
snapshot $object '
	tg.directory({
	  "bar": {
	    "graph": tg.graph({
	      "nodes": [
	        {
	          "kind": "directory",
	          "entries": {
	            "tangram.ts": 2,
	          },
	        },
	        {
	          "kind": "file",
	          "contents": tg.blob("import * as bar from \"../bar\";"),
	          "dependencies": {
	            "../bar": {
	              "node": 0,
	              "options": {
	                "path": "../bar",
	              },
	            },
	          },
	          "module": "ts",
	        },
	        {
	          "kind": "file",
	          "contents": tg.blob("import * as foo from \"../foo\";"),
	          "dependencies": {
	            "../foo": {
	              "node": 3,
	              "options": {
	                "path": "../foo",
	              },
	            },
	          },
	          "module": "ts",
	        },
	        {
	          "kind": "directory",
	          "entries": {
	            "tangram.ts": 1,
	          },
	        },
	      ],
	    }),
	    "index": 0,
	    "kind": "directory",
	  },
	  "foo": {
	    "graph": tg.graph({
	      "nodes": [
	        {
	          "kind": "directory",
	          "entries": {
	            "tangram.ts": 2,
	          },
	        },
	        {
	          "kind": "file",
	          "contents": tg.blob("import * as bar from \"../bar\";"),
	          "dependencies": {
	            "../bar": {
	              "node": 0,
	              "options": {
	                "path": "../bar",
	              },
	            },
	          },
	          "module": "ts",
	        },
	        {
	          "kind": "file",
	          "contents": tg.blob("import * as foo from \"../foo\";"),
	          "dependencies": {
	            "../foo": {
	              "node": 3,
	              "options": {
	                "path": "../foo",
	              },
	            },
	          },
	          "module": "ts",
	        },
	        {
	          "kind": "directory",
	          "entries": {
	            "tangram.ts": 1,
	          },
	        },
	      ],
	    }),
	    "index": 3,
	    "kind": "directory",
	  },
	})
'
