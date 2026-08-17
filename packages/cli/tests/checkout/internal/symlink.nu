use ../../../test.nu *

# Checking out a symlink with an absolute target path writes the symlink into the checkouts directory.

let server = spawn

# Create the artifact.
let artifact = '
	tg.symlink({
		"path": "/bin/sh"
	})
'
let id = tg put $artifact

# Check out.
tg checkout $id

# Snapshot.
snapshot --path $server.checkout_directory
