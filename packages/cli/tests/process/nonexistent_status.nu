use ../../test.nu *

# Requesting the status of a process that does not exist fails with a missing-process error.

let server = spawn

let output = tg status pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to get the process status
	   id = <process>
	-> failed to find the process

'
