use ../../test.nu *

# Eagerly pulling a graph directory pointer from a remote fails with an error indicating that a pointer was found instead of an object.

let remote = spawn --cloud --name remote
let local = spawn --name local
let output = tg remote put default $remote.url | complete
success $output

# Create a graph on the remote server with a single directory node.
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
let graph_id = tg --url $remote.url put $artifact | str trim
tg --url $remote.url index
let reference = $"graph=($graph_id)&index=0&kind=directory"
let output = tg pull $reference --eager | complete
failure $output 'expected an object, got a pointer'
