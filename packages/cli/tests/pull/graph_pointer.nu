use ../../test.nu *

let remote = spawn --cloud -n remote
let local = spawn -n local
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
