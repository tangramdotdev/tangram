use ../../test.nu *

# Touching an object by a well formed id that does not exist fails.

let server = spawn

let output = tg object touch fil_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to touch the object
	   id = fil_010000000000000000000000000000000000000000000000000000
	-> failed to find the object

'
