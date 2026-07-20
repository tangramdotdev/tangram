use ../../test.nu *

# Requesting the output of a process that does not exist fails with a missing-process error.

let server = spawn

let output = tg output pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to get the process output
	   id = pcs_010000000000000000000000000000000000000000000000000000
	-> failed to find the process

'
