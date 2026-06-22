use ../../test.nu *

# Getting an object by a well-formed id that does not exist fails.

let server = spawn

let output = tg get fil_0000000000000000000000000000 | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to load the object

'
