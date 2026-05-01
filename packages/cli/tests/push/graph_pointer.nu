use ../../test.nu *

let remote = spawn --cloud -n remote
let local = spawn -n local
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
