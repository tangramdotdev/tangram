use ../../test.nu *

# Touching a process by a well formed id that does not exist fails.

let server = spawn

let output = tg process touch pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to touch the process
	   id = <process>
	-> failed to find the process

'
