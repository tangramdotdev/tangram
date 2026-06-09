use ../../test.nu *

# Requesting the output of a process that does not exist fails with a missing-process error.

let server = spawn

let output = tg output pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
assert ($output.stderr | str contains 'failed to find the process') "the error should mention the missing process"
