use ../lib/test.nu *

# Reading the log of a process that does not exist fails because its stdio cannot be retrieved.

let local = server spawn --config { control: { read_timeout: 0.25 } }

let output = tg log pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to get the process stdio
	   id = pcs_010000000000000000000000000000000000000000000000000000

'
