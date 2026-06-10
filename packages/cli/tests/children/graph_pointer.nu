use ../../test.nu *

# Requesting the children of a graph directory pointer fails with an error indicating that a pointer was found instead of an object.

let server = spawn
let artifact = '
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
let graph_id = tg put $artifact | str trim
let reference = $"graph=($graph_id)&index=0&kind=directory"
let output = tg children $reference | complete
failure $output 'expected an object, got a pointer'
