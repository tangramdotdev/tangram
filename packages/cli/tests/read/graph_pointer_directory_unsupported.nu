use ../../test.nu *

let server = spawn

# Create a graph with a single directory node.
let graph = '
	tg.graph({
		"nodes": [
			{
				"kind": "directory",
				"entries": {
					"hello.txt": tg.file({ "contents": tg.blob("Hello, World!") })
				}
			}
		]
	})
'
let graph_id = tg put $graph | str trim

# Reading a directory pointer should fail.
let output = tg read $"graph=($graph_id)&index=0&kind=directory" | complete
failure $output 'cannot read a directory'
