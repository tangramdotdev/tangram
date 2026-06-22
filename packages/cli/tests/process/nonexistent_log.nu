use ../../test.nu *

# Reading the log of a process that does not exist fails because its stdio cannot be retrieved.

let server = spawn

let output = tg log pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to get the process stdio
	   id = <process>

'
