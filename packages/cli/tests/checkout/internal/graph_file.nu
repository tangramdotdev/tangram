use ../../../test.nu *

# Checking out a file defined through a graph node writes the file into the checkouts directory.

let server = server spawn

# Create the artifact.
let artifact = '
	tg.file({
		"graph": tg.graph({
			"nodes": [
				{
					"kind": "file",
					"contents": tg.blob("Hello, World!")
				}
			]
		}),
		"index": 0,
		"kind": "file"
	})
'
let id = tg put $artifact

# Check out.
tg checkout $id

# Snapshot.
snapshot --path $server.checkout_directory
