use ../../test.nu *

# Deleting a remote that does not exist fails with a missing-remote error.

let server = spawn

let output = tg remote delete nonexistent | complete
failure $output
assert ($output.stderr | str contains 'failed to find the remote') "the error should mention the missing remote"
