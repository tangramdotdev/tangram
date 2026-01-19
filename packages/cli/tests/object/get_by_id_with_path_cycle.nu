use ../../test.nu *

# Test getting from an object with ?path= when there's a cyclical import.

let server = spawn

let path = artifact {
	tangram.ts: 'import * as a from "./file.tg.ts";',
	file.tg.ts: 'import * as root from "./tangram.ts";',
}
let id = tg checkin $path
let output = tg get $"($id)?path=./file.tg.ts" --depth=inf --pretty | complete
success $output
snapshot $output.stdout '
	tg.file({
	  "graph": tg.graph({
	    "nodes": [
	      {
	        "kind": "file",
	        "contents": blb_010pwqd32ehjhaj9eswh61x95cgqby7x5w0fybj56a34cmbehs3mhg,
	        "dependencies": {
	          "./tangram.ts": {
	            "item": {
	              "index": 1,
	              "kind": "file",
	            },
	            "options": {
	              "path": "tangram.ts",
	            },
	          },
	        },
	        "module": "ts",
	      },
	      {
	        "kind": "file",
	        "contents": blb_01b7ka1dzz1k7n5fh52av0vxtkycf3z2kntyvnvv549x2xdy36mm9g,
	        "dependencies": {
	          "./file.tg.ts": {
	            "item": {
	              "index": 0,
	              "kind": "file",
	            },
	            "options": {
	              "path": "file.tg.ts",
	            },
	          },
	        },
	        "module": "ts",
	      },
	    ],
	  }),
	  "index": 0,
	  "kind": "file",
	})

'
