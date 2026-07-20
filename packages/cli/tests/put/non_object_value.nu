use ../../test.nu *

# Putting a value that is not an object fails because only objects have ids to print.

let server = spawn

let output = tg put '42' | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> expected an object value

'
