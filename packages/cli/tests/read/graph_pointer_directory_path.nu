use ../../test.nu *

let server = spawn

# Create a graph with a directory node that points to a file node.
let graph = '
	tg.graph({
		"nodes": [
			{
				"kind": "directory",
				"entries": {
					"hello.txt": { "index": 1, "kind": "file" }
				}
			},
			{
				"kind": "file",
				"contents": tg.blob("Hello, World!")
			}
		]
	})
'
let graph_id = tg put $graph | str trim

# Read through the directory pointer using a reference path option.
let output = tg read $"graph=($graph_id)&index=0&kind=directory?get=hello.txt" | complete
success $output
assert equal $output.stdout 'Hello, World!'
