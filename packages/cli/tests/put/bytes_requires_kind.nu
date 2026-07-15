use ../../test.nu *

# Putting raw bytes without an id requires the kind flag to compute the id.

let server = spawn

let output = tg put --bytes "hello" | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> kind must be set when using --bytes

'
