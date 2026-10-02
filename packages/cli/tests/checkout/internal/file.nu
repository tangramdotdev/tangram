use ../../lib/test.nu *

# Checking out a simple file writes the file into the checkouts directory.

let local = server spawn

# Create the artifact.
let artifact = '
	tg.file("Hello, World!")
'
let id = tg put --no-tokens $artifact | referent node

# Check out.
tg checkout $id

# Snapshot.
snapshot --path $local.checkout_directory
