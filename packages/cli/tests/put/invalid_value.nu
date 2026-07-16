use ../../test.nu *

# Putting input that does not parse as a value fails with a parse error.

let server = spawn

let output = tg put 'tg.bogus(((' | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to parse the value
	-> 

'
