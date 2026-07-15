use ../../test.nu *

# Signalling a process that does not exist fails with a missing-process error.

let server = spawn

let output = tg signal pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to signal the process
	   id = pcs_010000000000000000000000000000000000000000000000000000
	-> failed to find the process

'
