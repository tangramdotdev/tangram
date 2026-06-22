use ../../test.nu *

# Getting the raw bytes of an object that does not exist fails with a missing-object error.

let server = spawn

let output = tg object get fil_010000000000000000000000000000000000000000000000000000 --bytes | complete
failure $output
snapshot ($output.stderr | redact | normalize_ids) '
	error an error occurred
	-> failed to find the object
	   id = fil_010000000000000000000000000000000000000000000000000000

'
