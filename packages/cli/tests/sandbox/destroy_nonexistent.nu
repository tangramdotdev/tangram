use ../../test.nu *

# Destroying a sandbox that does not exist fails with a missing-sandbox error.

let server = spawn

let output = tg sandbox destroy sbx_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to destroy the sandbox
	   sandbox = sbx_010000000000000000000000000000000000000000000000000000
	-> failed to find the sandbox

'
