use ../../test.nu *

# Waiting on a process that does not exist fails with a missing-process error.

let server = spawn

let output = tg wait pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to wait for the process
	   id = pcs_010000000000000000000000000000000000000000000000000000
	-> failed to find the process

'
