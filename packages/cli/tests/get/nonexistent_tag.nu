use ../../test.nu *

# Getting a tag that does not exist fails.

let server = spawn

let output = tg get nonexistent-tag | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to get the reference
	   reference = nonexistent-tag
	-> failed to get the reference
	   reference = nonexistent-tag

'
