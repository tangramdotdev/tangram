use ../../test.nu *

# Reading a well formed file id that does not exist fails.

let server = spawn

let output = tg read fil_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to get file contents
	-> failed to load the object

'
