use ../../test.nu *

# Checking in a path that does not exist fails to canonicalize the path.

let server = spawn

let output = tg checkin /nonexistent/path/here | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to canonicalize the path
	-> No such file or directory (os error 2)

'
