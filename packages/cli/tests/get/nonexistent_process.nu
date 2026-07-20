use ../../test.nu *

# Getting a process by a well-formed id that does not exist fails.

let server = spawn

let output = tg get pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to find the process
	   id = pcs_010000000000000000000000000000000000000000000000000000

'
