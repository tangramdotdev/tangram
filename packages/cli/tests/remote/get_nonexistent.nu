use ../../test.nu *

# Getting a remote that does not exist fails with a missing-remote error.

let server = spawn

let output = tg remote get nonexistent | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to find the remote
	   name = nonexistent

'
