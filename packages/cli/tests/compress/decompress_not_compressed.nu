use ../../test.nu *

# Decompressing a blob that is not compressed fails with an invalid compression format error.

let server = spawn

let blob = "hello, world!\n" | tg write

let output = tg decompress $blob | complete
failure $output
snapshot ($output.stderr | redact | normalize_ids) '
	error an error occurred
	-> the process failed
	   id = <process>
	-> invalid compression format

'
