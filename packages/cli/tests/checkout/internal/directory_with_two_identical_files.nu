use ../../../test.nu *

# Checking out a directory containing two entries with identical contents writes the directory into the checkouts directory.

let server = spawn

# Create the artifact.
let artifact = '
	tg.directory({
		"hello.txt": "Hello, World!",
		"world.txt": "Hello, World!"
	})
'
let id = tg put $artifact

# Check out.
tg checkout $id

# Snapshot.
snapshot --path $server.checkout_directory
