use ../../test.nu *

let server = spawn
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
let graph_id = tg put $artifact | str trim
let output = tg document $"graph=($graph_id)&index=0&kind=file?path=tangram.ts" | complete
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
	                "item": "graph=gph_01ky68qhkg8cap44t3dh5h34tp7s3z3qykcj8n32bx7rsnyv30k4fg&index=0&kind=file",
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
	                        "item": "graph=gph_01ky68qhkg8cap44t3dh5h34tp7s3z3qykcj8n32bx7rsnyv30k4fg&index=0&kind=file",
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
