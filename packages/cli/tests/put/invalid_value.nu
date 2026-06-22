use ../../test.nu *

# Putting input that does not parse as a value fails with a parse error.

let server = spawn

let output = tg put 'tg.bogus(((' | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to parse the value
	-> 

'
