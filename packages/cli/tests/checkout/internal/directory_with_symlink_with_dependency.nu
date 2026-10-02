use ../../lib/test.nu *

# Checking out a directory containing a symlink whose artifact is a dependency file writes the directory into the checkouts directory.

let local = server spawn

# Create the artifact.
let artifact = '
	tg.directory({
		"foo": tg.symlink({
			"artifact": tg.file("bar")
		})
	})
'
let id = tg put --no-tokens $artifact | referent node

# Check out.
tg checkout $id

# Snapshot.
snapshot --path $local.checkout_directory
