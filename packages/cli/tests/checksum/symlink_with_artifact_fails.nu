use ../../test.nu *

# Checksumming a directory that contains a symlink with an artifact target fails.

let server = spawn

let dir = tg put 'tg.directory({ "link": tg.symlink({ "artifact": tg.file("target") }) })' | str trim

let output = tg checksum $dir | complete
failure $output
snapshot ($output.stderr | redact | normalize_ids) '
	error an error occurred
	-> the process failed
	   id = <process>
	-> cannot checksum a symlink with an artifact

'
