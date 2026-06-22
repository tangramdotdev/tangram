use ../../test.nu *

# Getting the children of an object by a well formed id that does not exist fails.

let server = spawn

let output = tg object children fil_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot ($output.stderr | redact | normalize_ids) '
	error an error occurred
	-> failed to find the object
	   id = fil_010000000000000000000000000000000000000000000000000000

'
