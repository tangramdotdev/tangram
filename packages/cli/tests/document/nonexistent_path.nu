use ../../test.nu *

# Documenting a path that does not exist fails.

let server = spawn

let output = tg document /nonexistent/path/nowhere | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to canonicalize the path
	-> No such file or directory (os error 2)

'
