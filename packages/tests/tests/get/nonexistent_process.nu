use ../../util/test.nu *

# Getting a process by a well-formed id that does not exist fails.

let local = server spawn --config { control: { read_timeout: 0.25 } }

let output = tg get pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to get the process
	   id = pcs_010000000000000000000000000000000000000000000000000000
	-> failed to get the process

'
