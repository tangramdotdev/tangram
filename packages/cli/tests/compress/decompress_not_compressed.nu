use ../../test.nu *

# Decompressing a blob that is not compressed fails with an invalid compression format error.

let server = spawn

let blob = "hello, world!\n" | tg write

let output = tg decompress $blob | complete
failure $output
snapshot --normalize-ids $output.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> invalid compression format

'
