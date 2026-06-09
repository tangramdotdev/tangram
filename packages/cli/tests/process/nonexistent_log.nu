use ../../test.nu *

# Reading the log of a process that does not exist fails because its stdio cannot be retrieved.

let server = spawn

let output = tg log pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
assert ($output.stderr | str contains 'failed to get the process stdio') "the error should mention the missing process stdio"
