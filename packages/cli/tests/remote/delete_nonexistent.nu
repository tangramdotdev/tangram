use ../../test.nu *

# Deleting a remote that does not exist fails with a missing-remote error.

let server = spawn

let output = tg remote delete nonexistent | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to delete the remote
	   name = nonexistent
	-> failed to find the remote

'
