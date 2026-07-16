use ../../test.nu *

# Checksumming a directory that contains a file with dependencies fails.

let server = spawn

let dir = tg put 'tg.directory({ "f": tg.file({ "contents": tg.blob("x"), "dependencies": { "dep": { "item": tg.file("d") } } }) })' | str trim

let output = tg checksum $dir | complete
failure $output
snapshot --normalize-ids $output.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> cannot checksum a file with dependencies

'
