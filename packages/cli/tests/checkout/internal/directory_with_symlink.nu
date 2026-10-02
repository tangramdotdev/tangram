use ../../lib/test.nu *

# Checking out a directory containing a symlink that targets a sibling file writes the directory into the checkouts directory.

let local = server spawn

# Create the artifact.
let artifact = '
	tg.directory({
		"hello.txt": "Hello, World!",
		"link": tg.symlink({
			"path": "hello.txt"
		})
	})
'
let id = tg put --no-tokens $artifact | referent node

# Check out.
tg checkout $id

# Snapshot.
snapshot --path $local.checkout_directory
