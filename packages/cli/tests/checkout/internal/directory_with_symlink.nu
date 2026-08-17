use ../../../test.nu *

# Checking out a directory containing a symlink that targets a sibling file writes the directory into the checkouts directory.

let server = spawn

# Create the artifact.
let artifact = '
	tg.directory({
		"hello.txt": "Hello, World!",
		"link": tg.symlink({
			"path": "hello.txt"
		})
	})
'
let id = tg put $artifact

# Check out.
tg checkout $id

# Snapshot.
snapshot --path $server.checkout_directory
