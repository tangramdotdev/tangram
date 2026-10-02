use ../../lib/test.nu *

# Checking out an executable file writes the file into the checkouts directory with its executable bit preserved.

let local = server spawn

# Create the artifact.
let artifact = '
	tg.file({
		"contents": "Hello, World!",
		"executable": true
	})
'
let id = tg put --no-tokens $artifact | referent node

# Check out.
tg checkout $id

# Snapshot.
snapshot --path $local.checkout_directory
