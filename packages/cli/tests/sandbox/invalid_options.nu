use ../../test.nu *

# Malformed mount and isolation options are rejected at argument parsing.

let server = spawn

let output = tg sandbox create --mount "::bad::" | complete
failure $output
assert ($output.stderr | str contains "expected an absolute path") "the error should mention the absolute path"

let output = tg sandbox create --isolation warp | complete
failure $output
assert ($output.stderr | str contains "invalid isolation") "the error should mention the invalid isolation"
