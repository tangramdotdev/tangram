use ../../test.nu *

# Touching a process by a well formed id that does not exist fails.

let server = spawn

let output = tg process touch pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to touch the process
	   id = pcs_010000000000000000000000000000000000000000000000000000
	-> failed to find the process

'
