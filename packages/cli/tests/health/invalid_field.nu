use ../../test.nu *

# Requesting an invalid health field is an error.

let server = spawn

let output = tg health --fields bogus | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to get the health
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to get the server health
	-> invalid health field
	   field = bogus

'
