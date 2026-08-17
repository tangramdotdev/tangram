use ../../../test.nu *

# Checking out an executable file writes the file into the checkouts directory with its executable bit preserved.

let server = spawn

# Create the artifact.
let artifact = '
	tg.file({
		"contents": "Hello, World!",
		"executable": true
	})
'
let id = tg put $artifact

# Check out.
tg checkout $id

# Snapshot.
snapshot --path $server.checkout_directory
