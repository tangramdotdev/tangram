use ../../test.nu *

# Getting the raw bytes of an object that does not exist fails with a missing-object error.

let server = spawn

let output = tg object get fil_010000000000000000000000000000000000000000000000000000 --bytes | complete
failure $output
snapshot --normalize-ids $output.stderr '
	error an error occurred
	-> failed to find the object
	   id = fil_010000000000000000000000000000000000000000000000000000

'
