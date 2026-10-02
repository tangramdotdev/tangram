use ../lib/test.nu *

# Reading a directory object fails.

let local = server spawn

let dir = tg put --no-tokens 'tg.directory({ "f": tg.file("x") })' | referent node

let output = tg read $dir | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> expected a blob, file, or symlink that points to a file

'
