use ../../test.nu *

# Checksumming a directory that contains a symlink with an artifact target fails.

let server = spawn

let dir = tg put 'tg.directory({ "link": tg.symlink({ "artifact": tg.file("target") }) })' | str trim

let output = tg checksum $dir | complete
failure $output
snapshot --normalize-ids $output.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> cannot checksum a symlink with an artifact

'
