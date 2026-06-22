use ../../test.nu *

# Object metadata for a well formed id that does not exist fails.

let server = spawn

let output = tg object metadata fil_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to find the object metadata
	   id = fil_010000000000000000000000000000000000000000000000000000

'
