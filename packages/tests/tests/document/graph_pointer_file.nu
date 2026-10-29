use ../../util/test.nu *

# Documenting a graph file pointer treated as a tangram.ts module produces documentation JSON that matches the snapshot.

let local = server spawn
let artifact = '
	tg.graph({
		"nodes": [
			{
				"kind": "file",
				"contents": tg.blob("export default () => 42;")
			}
		]
	})
'
let graph_id = tg put --no-tokens $artifact | referent node
let output = tg document $"graph=($graph_id)&index=0&kind=file?get=tangram.ts" | complete
success $output

let json = $output.stdout | from json
snapshot ($json | to json --indent 2) '
	{
	  "declarations": [
	    {
	      "kind": "function",
	      "location": {
	        "module": 0,
	        "range": [
	          0,
	          0,
	          0,
	          24
	        ]
	      },
	      "name": "default",
	      "signature": {
	        "location": {
	          "module": 0,
	          "range": [
	            0,
	            15,
	            0,
	            23
	          ]
	        },
	        "return": {
	          "kind": "keyword",
	          "value": "number"
	        }
	      }
	    }
	  ],
	  "exports": {
	    "default": [
	      0
	    ]
	  },
	  "modules": [
	    {
	      "kind": "typescript",
	      "referent": {
	        "node": "graph=gph_015x47jbz5vyx04p9635q73syh4ermxzwkn6bd4w6dpfynnfsg73w0&index=0&kind=file",
	        "options": {
	          "path": "tangram.ts"
	        }
	      }
	    }
	  ]
	}
'
