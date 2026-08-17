use ../../../test.nu *

# Checking out a simple file writes the file into the checkouts directory.

let server = spawn

# Create the artifact.
let artifact = '
	tg.file("Hello, World!")
'
let id = tg put $artifact

# Check out.
tg checkout $id

# Snapshot.
snapshot --path $server.checkout_directory
