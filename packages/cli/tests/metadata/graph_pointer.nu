use ../../test.nu *

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
