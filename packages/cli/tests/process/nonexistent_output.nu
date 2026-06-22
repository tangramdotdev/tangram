use ../../test.nu *

# Requesting the output of a process that does not exist fails with a missing-process error.

let server = spawn

let output = tg output pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to get the process output
	   id = <process>
	-> failed to find the process

'
