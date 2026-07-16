use ../../test.nu *

# Process metadata for a well formed id that does not exist fails.

let server = spawn

let output = tg process metadata pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to find the process metadata
	   id = pcs_010000000000000000000000000000000000000000000000000000

'
