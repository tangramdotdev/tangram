use ../../util/test.nu *

# Documenting a graph directory pointer containing a tangram.ts module produces documentation JSON that matches the snapshot.

let local = server spawn

# Create a graph with a directory node containing a tangram.ts module.
let artifact = '
	tg.graph({
		"nodes": [
			{
				"kind": "directory",
				"entries": {
					"tangram.ts": tg.file({ "contents": tg.blob("export default () => 42;") })
				}
			}
		]
	})
'
let graph_id = tg put --no-tokens $artifact | referent node
let output = tg document $"graph=($graph_id)&index=0&kind=directory" | complete
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
	        "node": "fil_01c3d141vk7v44j4krd8800sc11z2ddfyr4x7xp8z8r778r4rb4qr0",
	        "options": {
	          "path": "tangram.ts"
	        }
	      }
	    }
	  ]
	}
'
