use ../../test.nu *

# Outdated fails for a path that does not exist.

let server = spawn

let output = tg outdated /nonexistent/path/nowhere | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to find the root
	-> failed to get the metadata
	   path = /nonexistent/path/nowhere
	-> No such file or directory (os error 2)

'
