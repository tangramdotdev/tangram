use ../../util/test.nu *

# Signalling a process that does not exist fails with a missing-process error.

let local = server spawn --config { control: { read_timeout: 0.25 } }

let output = tg signal pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to signal the process
	   id = pcs_010000000000000000000000000000000000000000000000000000
	-> failed to find the process

'
