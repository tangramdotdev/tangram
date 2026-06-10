use ../../test.nu *

# Getting a tag that does not exist fails.

let server = spawn

let output = tg get nonexistent-tag | complete
failure $output
assert ($output.stderr | str contains "failed to get the reference") "the error should mention the failed get"
