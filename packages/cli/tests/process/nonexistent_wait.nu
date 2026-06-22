use ../../test.nu *

# Waiting on a process that does not exist fails with a missing-process error.

let server = spawn

let output = tg wait pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to wait for the process
	   id = <process>
	-> failed to find the process

'
