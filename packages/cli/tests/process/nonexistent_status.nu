use ../../test.nu *

# Requesting the status of a process that does not exist fails with a missing-process error.

let server = server spawn

let output = tg process status pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to get the process status
	   id = pcs_010000000000000000000000000000000000000000000000000000
	-> failed to get the process

'
