use ../../test.nu *

# Putting input that does not parse as a value fails with a parse error.

let server = spawn

let output = tg put 'tg.bogus(((' | complete
failure $output
assert ($output.stderr | str contains "failed to parse the value") "the error should mention the value parse failure"
