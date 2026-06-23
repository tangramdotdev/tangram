use ../../test.nu *

# Requesting compression with the zip format fails, because zip archives have their own internal compression.

let server = spawn

let dir = tg put 'tg.directory({ "hello.txt": tg.file("hello") })' | str trim

let output = tg archive --format zip --compression gz $dir | complete
failure $output
snapshot ($output.stderr | redact | normalize_ids) '
	error an error occurred
	-> the process failed
	   id = <process>
	-> compression is not supported for zip archives

'
