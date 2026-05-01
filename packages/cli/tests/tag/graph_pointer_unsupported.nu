use ../../test.nu *

let server = spawn

# Put a graph and capture its id.
let graph = '
	tg.graph({
		"nodes": [
			{
				"kind": "directory",
				"entries": {
					"hello.txt": tg.file("Hello, World!")
				}
			}
		]
	})
'
let graph_id = tg put $graph

# Attempt to tag a graph pointer reference. This should fail because tagging
# graph pointers is unsupported.
let output = tg tag put test $"graph=($graph_id)&index=0&kind=directory" | complete
failure $output 'cannot tag a graph pointer'
