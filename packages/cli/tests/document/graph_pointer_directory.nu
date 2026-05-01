use ../../test.nu *

let server = spawn

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
let graph_id = tg put $artifact | str trim
let output = tg document $"graph=($graph_id)&index=0&kind=directory" | complete
success $output
let json = $output.stdout | from json
snapshot ($json | to json -i 2) '
	{
	  "exports": {
	    "default": {
	      "variable": [
	        {
	          "comment": {
	            "tags": [],
	            "text": ""
	          },
	          "location": {
	            "module": {
	              "kind": "ts",
	              "referent": {
	                "item": "fil_01c3d141vk7v44j4krd8800sc11z2ddfyr4x7xp8z8r778r4rb4qr0",
	                "options": {
	                  "path": "tangram.ts"
	                }
	              }
	            },
	            "range": {
	              "end": {
	                "character": 24,
	                "line": 0
	              },
	              "start": {
	                "character": 0,
	                "line": 0
	              }
	            }
	          },
	          "type": {
	            "kind": "function",
	            "value": {
	              "signatures": [
	                {
	                  "location": {
	                    "module": {
	                      "kind": "ts",
	                      "referent": {
	                        "item": "fil_01c3d141vk7v44j4krd8800sc11z2ddfyr4x7xp8z8r778r4rb4qr0",
	                        "options": {
	                          "path": "tangram.ts"
	                        }
	                      }
	                    },
	                    "range": {
	                      "end": {
	                        "character": 23,
	                        "line": 0
	                      },
	                      "start": {
	                        "character": 15,
	                        "line": 0
	                      }
	                    }
	                  },
	                  "parameters": {},
	                  "return": {
	                    "kind": "keyword",
	                    "value": "number"
	                  },
	                  "typeParameters": {}
	                }
	              ]
	            }
	          }
	        }
	      ]
	    }
	  }
	}
'
