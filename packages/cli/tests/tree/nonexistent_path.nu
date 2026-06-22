use ../../test.nu *

# Displaying a tree for a path that does not exist fails.

let server = spawn

let output = tg tree /nonexistent/path/nowhere | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to canonicalize the path
	-> No such file or directory (os error 2)

'
