use ../lib/test.nu *

# Putting a value that is not an object fails because only objects have ids to print.

let local = server spawn

let output = tg put --no-tokens '42' | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> expected an object value

'
