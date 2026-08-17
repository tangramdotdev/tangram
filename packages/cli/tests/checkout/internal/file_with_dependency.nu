use ../../../test.nu *

# Checking out a file with a dependency on another file writes the file into the checkouts directory.

let server = spawn

# Create the artifact.
let artifact = '
	tg.file({
		"contents": "foo",
		"dependencies": {
			"bar": {
				"node": tg.file("bar")
			}
		}
	})
'
let id = tg put $artifact

# Check out.
tg checkout $id

# Snapshot.
snapshot --path $server.checkout_directory
