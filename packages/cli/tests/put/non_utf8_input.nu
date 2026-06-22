use ../../test.nu *

# Putting non-utf-8 input without the bytes flag fails because values must be parsed from text.

let server = spawn

let output = 0x[ff fe fd] | tg put | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> the input was not valid utf-8
	-> invalid utf-8 sequence of 1 bytes from index 0

'
