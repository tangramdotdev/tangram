use ../../test.nu *

# Getting a reference that does not parse fails with a parse error.

let server = spawn

let output = tg get 'not a reference' | complete
failure $output
assert ($output.stderr | str contains 'failed to parse the reference') "the error should mention the reference parse failure"
