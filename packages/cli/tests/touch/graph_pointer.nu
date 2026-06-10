use ../../test.nu *

# Touching a graph directory pointer fails with an error indicating that a pointer was found instead of an object.

let server = spawn

let graph = '
	tg.graph({
		"nodes": [
			{
				"kind": "directory",
				"entries": {}
			}
		]
	})
'
let graph_id = tg put $graph

let output = tg touch $"graph=($graph_id)&index=0&kind=directory" | complete
failure $output 'expected an object, got a pointer'
