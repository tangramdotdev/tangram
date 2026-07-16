use ../../test.nu *

# Cancelling a process that does not exist fails with a missing-process error.

let server = spawn

let output = tg cancel pcs_0000000000000000000000000000 sometoken | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to cancel the process
	   id = pcs_0000000000000000000000000000
	-> failed to find the process

'
