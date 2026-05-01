use ../../test.nu *

let server = spawn

# Create a graph with a single file node.
let graph = '
	tg.graph({
		"nodes": [
			{
				"kind": "file",
				"contents": tg.blob("hello")
			}
		]
	})
'
let graph_id = tg put $graph

# Checksumming a graph pointer should fail.
let output = tg checksum $"graph=($graph_id)&index=0&kind=file" | complete
failure $output 'checksumming graph pointers is unsupported'
