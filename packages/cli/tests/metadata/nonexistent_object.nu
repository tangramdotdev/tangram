use ../../test.nu *

# Object metadata for a well formed id that does not exist fails.

let server = server spawn

let output = tg object metadata fil_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to get the object metadata
	   id = fil_010000000000000000000000000000000000000000000000000000
	-> failed to get the object metadata

'
