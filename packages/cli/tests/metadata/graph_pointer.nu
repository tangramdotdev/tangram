use ../../test.nu *

# Requesting metadata for a graph directory pointer fails with an error indicating that a pointer was found instead of an object.

let server = spawn
let artifact = '
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
let graph_id = tg put $artifact
tg index
let reference = $"graph=($graph_id)&index=0&kind=directory"
let output = tg metadata $reference --pretty | complete
failure $output 'expected an object, got a pointer'
