use ../../test.nu *

# Writing process stdio without exactly one stream selected fails.

let server = spawn

let output = "data" | tg process stdio write pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> expected exactly one stdio stream

'
