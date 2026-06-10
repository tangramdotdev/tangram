use ../../test.nu *

# Destroying a sandbox that does not exist fails with a missing-sandbox error.

let server = spawn

let output = tg sandbox destroy sbx_010000000000000000000000000000000000000000000000000000 | complete
failure $output
assert ($output.stderr | str contains "failed to find the sandbox") "the error should mention the missing sandbox"
