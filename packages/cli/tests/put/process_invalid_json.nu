use ../../test.nu *

# Putting a process with input that is not valid process data fails with a deserialization error.

let server = spawn

let output = tg put --id pcs_010000000000000000000000000000000000000000000000000000 "not json" | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to deserialize the data
	-> expected ident at line 1 column 2

'
