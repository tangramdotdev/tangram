use ../../test.nu *

let server = spawn

# Put a graph and capture its id.
let value = '
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
let graph_id = tg put $value

# Build a graph pointer reference and attempt to cache it.
let reference = $"graph=($graph_id)&index=0&kind=directory"
let output = tg cache $reference | complete

# The command should fail with the expected message.
failure $output
let stderr = $output.stderr | lines | last
snapshot $stderr '-> caching graph artifacts is unsupported'
