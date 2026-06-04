use ../../test.nu *

# Getting a graph directory pointer fails with an error indicating that a pointer was found instead of an object.

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
success $output
assert equal ($output.stdout | lines) [
	"{"
	$'  "graph": "($graph)",'
	'  "index": 0,'
	'  "kind": "directory",'
	"}"
]
