use ../../test.nu *

let server = spawn

# Create a graph with a symlink node at index 0 that points to a file node at index 1.
let graph = '
	tg.graph({
		"nodes": [
			{
				"kind": "symlink",
				"artifact": { "index": 1, "kind": "file" }
			},
			{
				"kind": "file",
				"contents": tg.blob("Hello, World!")
			}
		]
	})
'
let graph_id = tg put $graph | str trim

# Read through the symlink pointer and assert the resolved file contents.
let output = tg read $"graph=($graph_id)&index=0&kind=symlink" | complete
success $output
assert equal $output.stdout 'Hello, World!'
