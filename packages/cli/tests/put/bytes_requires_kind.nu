use ../../test.nu *

# Putting raw bytes without an id requires the kind flag to compute the id.

let server = spawn

let output = tg put --bytes "hello" | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> kind must be set when using --bytes

'
