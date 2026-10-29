use ../../util/test.nu *

# Cancelling a process that does not exist fails with a missing-process error.

let local = server spawn --config { control: { read_timeout: 0.25 } }

let output = tg cancel pcs_0000000000000000000000000000 06d0000001r010000000000000 | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to cancel the process
	   id = pcs_0000000000000000000000000000
	-> failed to find the process

'
