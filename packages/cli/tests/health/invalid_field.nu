use ../../test.nu *

# Requesting an invalid health field is an error.

let server = spawn

let output = tg health --fields bogus | complete
failure $output
assert ($output.stderr | str contains 'invalid health field') "the error should mention the invalid field"
