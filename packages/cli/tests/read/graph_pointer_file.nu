use ../../test.nu *

let server = spawn

# Create a graph with a single file node.
let graph = '
	tg.graph({
		"nodes": [
			{
				"kind": "file",
				"contents": tg.blob("Hello, World!")
			}
		]
	})
'
let graph_id = tg put $graph | str trim

# Read the file pointer and assert the contents match.
let output = tg read $"graph=($graph_id)&index=0&kind=file" | complete
success $output
assert equal $output.stdout 'Hello, World!'
