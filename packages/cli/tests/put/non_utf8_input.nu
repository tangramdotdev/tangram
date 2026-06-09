use ../../test.nu *

# Putting non-utf-8 input without the bytes flag fails because values must be parsed from text.

let server = spawn

let output = 0x[ff fe fd] | tg put | complete
failure $output
assert ($output.stderr | str contains "the input was not valid utf-8") "the error should mention the utf-8 requirement"
