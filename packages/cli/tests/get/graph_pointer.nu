use ../../test.nu *

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
let graph = tg put $artifact | str trim
let output = tg get $"graph=($graph)&index=0&kind=directory" --pretty | complete
failure $output 'expected an object, got a pointer'
