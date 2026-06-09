use ../../test.nu *

# Deleting a group that does not exist fails with a missing-group error.

let server = spawn

let output = tg group delete nonexistent | complete
failure $output
assert ($output.stderr | str contains 'failed to find the group') "the error should mention the missing group"
