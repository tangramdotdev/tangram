use ../../test.nu *

# Reading a directory object fails.

let server = spawn

let dir = tg put 'tg.directory({ "f": tg.file("x") })' | str trim

let output = tg read $dir | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> expected a blob, file, or symlink that points to a file

'
