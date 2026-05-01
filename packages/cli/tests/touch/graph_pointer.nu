use ../../test.nu *

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
