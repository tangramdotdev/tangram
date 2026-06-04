use ../../test.nu *

# Eagerly pushing a graph pointer reference fails with an error reporting that an object was expected but a pointer was given.

let remote = spawn --cloud --name remote
let local = spawn --name local
let output = tg remote put default $remote.url | complete
success $output
let artifact = '
	tg.graph({
		"nodes": [
			{
				"kind": "directory",
				"entries": {
					"hello.txt": tg.file("Hello, world"),
				}
			}
		]
	})
'
let graph_id = tg put $artifact
let reference = $"graph=($graph_id)&index=0&kind=directory"
let output = tg push $reference --eager | complete
failure $output 'expected an object, got a pointer'
