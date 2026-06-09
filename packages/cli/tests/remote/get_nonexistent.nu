use ../../test.nu *

# Getting a remote that does not exist fails with a missing-remote error.

let server = spawn

let output = tg remote get nonexistent | complete
failure $output
assert ($output.stderr | str contains "failed to find the remote") "the error should mention the missing remote"
