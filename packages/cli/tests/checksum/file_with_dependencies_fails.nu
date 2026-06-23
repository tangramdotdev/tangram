use ../../test.nu *

# Checksumming a directory that contains a file with dependencies fails.

let server = spawn

let dir = tg put 'tg.directory({ "f": tg.file({ "contents": tg.blob("x"), "dependencies": { "dep": { "item": tg.file("d") } } }) })' | str trim

let output = tg checksum $dir | complete
failure $output
snapshot ($output.stderr | redact | normalize_ids) '
	error an error occurred
	-> the process failed
	   id = <process>
	-> cannot checksum a file with dependencies

'
