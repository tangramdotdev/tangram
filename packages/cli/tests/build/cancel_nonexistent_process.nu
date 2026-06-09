use ../../test.nu *

# Cancelling a process that does not exist fails with a missing-process error.

let server = spawn

let output = tg cancel pcs_0000000000000000000000000000 sometoken | complete
failure $output
assert ($output.stderr | str contains 'failed to find the process') "the error should mention the missing process"
